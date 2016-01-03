var _ = require('lodash');
var q = require('q');
var flatten = require('flat');
var extend = require('util')._extend;
var levelup = require('levelup');
var crypto = require('crypto');
var es = require('event-stream');
var memdown = require('memdown');
// var db = levelup('./mydb');
var db;// = levelup({ db: memdown });
var promiseChain = q.resolve(true);
// db = levelup('./levelUpDistinct');

require('memdown').clearGlobalStore(true);

var storage = {};

var defaultPropertyClassifier = {
	isNull: function (data) {
		return data === null;
	},
	isZero: function (data) {
		return data === 0;
	},
	isEmptyString: function (data) {
		return data === '';
	},
	dataType: function (data) {
		return typeof data;
	},
	log10Length: function (data) {
		//this is applicable for both array and string.
		return (typeof(data) === 'string' || Array.isArray(data)) && Math.round(Math.log(data.length) / Math.LN10);
	},
	log10: function (data) {
		//this is applicable for numbers
		return typeof(data) === 'number'  && data > 0 && Math.round(Math.log(data) / Math.LN10);
	},
	objectPropertyCount: function (data) {
		return (Object.prototype.toString.call(data) === '[object Object]') && Object.keys(data).length;
	}
};

function hash (data) {
    return crypto.createHash('md5').update(JSON.stringify(data)).digest('hex');
}

function valueClass (data, predicateSet) {
	var predicatesResult = _.mapValues(predicateSet, function (predicate) {
		return predicate.call(null, data);
	})

	return hash(predicatesResult);
}

function classifyProperties (data, predicateSet) {
	if(Array.isArray(data)) {
		return data.map(function(dataItem) {
				return classifyProperties(data, predicateSet)
			}).concat(valueClass(dataItem, predicateSet));
	};

	if (Object.prototype.toString.call(data) === '[object Object]') {
		return extend(
			_.mapValues(data, function(dataItem) {
				return classifyProperties(dataItem, predicateSet)
			}),
			{self: valueClass(data, predicateSet)}
		);
	}

	return valueClass(data, predicateSet)
}

function classifyFeatures (propertyClasses, features) {
	return _.mapValues(features, function (featureProperties) {
		var dependencyClass = featureProperties.map(function (propertyPath) {
				return propertyClasses[propertyPath];
			});

		return hash(dependencyClass);
	});
}

var createFilterDistinct = function (options) {
	//by default classify all properties of all objects
	var propertyExtractor = options && options.propertyExtractor || _.identity;
	//by default do not use groups
	var groupExtractor = options && options.groupExtractor || _.constant('default');
	//by default all properties are independent, meaning that combinations are not considered significant
	var features = options && options.features || {};

    if (options && options.preserve) {
        db = levelup('./levelUpDistinct');
    } else {
        db = levelup({ db: memdown });
    }

	return function (data, callback) {
		//remove properties that have value 'undefined';
		data = JSON.parse(JSON.stringify(data));

	  	var group = groupExtractor(data);
	  	var extractedProperties = propertyExtractor(data);
	  	var propertyClasses = classifyProperties(extractedProperties, defaultPropertyClassifier);
	  	var featureClasses = classifyFeatures(propertyClasses, features);
	  	var lookupKeys = flatten({
	  			properties: propertyClasses,
	  			features: featureClasses
	  	});
	  	// console.log('LOOKUPKEYS',lookupKeys)
	  	//if any property or feature appears to be unique, then paas through the data.
        // var someNotFound;

        // promiseChain = promiseChain
        //         .then(function () {
        //             console.log('setsomenotfoundfalse')
        //             someNotFound = false;
        //         })

        var lookupPromises = [];
        var lookupPromiseKeys = [];

	  	Object.keys(lookupKeys).forEach(function (key) {
	  		var lookupKey = ('group:' + group + ', key: '+ key + ', class:' + lookupKeys[key]);

            lookupPromises.push(q.ninvoke(db, 'get', lookupKey));
            lookupPromiseKeys.push(lookupKey);
      //       promiseChain = promiseChain
		  		// .then(function () {
		  		// 	return q.ninvoke(db, 'get', lookupKey);
		  		// })
      //           .catch(function () {
      //               someNotFound = true;
      //               return q.ninvoke(db, 'put', lookupKey, true);
      //           })
      //           .catch(function(err) {console.log(err)})
        });

        promiseChain = promiseChain
            .then(function () {
                return q.allSettled(lookupPromises)
                    .then(function (results) {
                        var batchOperations = [];

                        results.forEach(function (result, index) {
                            if (result.state === "rejected") {
                                console.log(result)
                                batchOperations.push({type:'put', key: lookupPromiseKeys[index], value:true});
                            }
                        });

                        if (batchOperations.length === 0) {
                            //all properties and features were already covered, report as not distinct
console.log('NOT DISTINCT');
                            callback(null, false);
                            return;
                        }
                        return q.ninvoke(db, 'batch', batchOperations)
                            .then(function () {
                                console.log(batchOperations)
                                console.log('DISTINCT');
                                //some properties or features were not found, report as distinct
                                callback(null, true)
                            })
                    })
                    // .catch(callback);
            })
            .catch(console.log)
	};
}

module.exports.create = createFilterDistinct;
