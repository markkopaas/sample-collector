var _ = require('lodash');
var q = require('q');
var flatten = require('flat');
var extend = require('util')._extend;
var levelup = require('levelup');
var classify = require('./classify');

var db;

//seed the chain
var promiseChain = q.resolve();

var storage = {};

function classifyProperties (data) {
	if(Array.isArray(data)) {
		return data.map(function(dataItem) {
				return classifyProperties(data)
			}).concat(classify(dataItem));
	};

	if (Object.prototype.toString.call(data) === '[object Object]') {
		return extend(
			_.mapValues(data, function(dataItem) {
				return classifyProperties(dataItem)
			}),
			{self: classify(data)}
		);
	}

	return classify(data)
}

function classifyFeatures (propertyClasses, features) {
	return _.mapValues(features, function (featureProperties) {
		var dependencyClass = featureProperties.map(function (propertyPath) {
				return _.get(propertyClasses,propertyPath);
			});

		return JSON.stringify(dependencyClass);
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
        var memdown = require('memdown');
        memdown.clearGlobalStore(true);

        db = levelup({ db: memdown });
    }

	return function (data) {
		//remove properties that have value 'undefined';
		data = JSON.parse(JSON.stringify(data));

	  	var group = groupExtractor(data);
	  	var extractedProperties = propertyExtractor(data);
	  	var propertyClasses = classifyProperties(extractedProperties);
	  	var featureClasses = classifyFeatures(propertyClasses, features);
	  	var lookupKeys = flatten({
	  			properties: propertyClasses,
	  			features: featureClasses
	  	});

        var lookupPromises = [];
        var lookupPromiseKeys = [];

	  	Object.keys(lookupKeys).forEach(function (key) {
	  		var lookupKey = ('group:' + group + ', key: '+ key + ', class:' + lookupKeys[key]);

            lookupPromises.push(q.ninvoke(db, 'get', lookupKey));
            lookupPromiseKeys.push(lookupKey);
        });

        promiseChain = promiseChain
            .then(function () {
                return q.allSettled(lookupPromises);
            })
            .then(function (results) {
                var batchOperations = [];

                results.forEach(function (result, index) {
                    if (result.state === "rejected") {
                        batchOperations.push({type:'put', key: lookupPromiseKeys[index], value:true});
                    }
                });

                if (batchOperations.length === 0) {
                    //all properties and features were already covered, report as not distinct
                    return false;
                }
                return q.ninvoke(db, 'batch', batchOperations)
                .then(function () {
                    //some properties or features were not found, report as distinct
                    return true
                })
            })

        return promiseChain;
	};
}

module.exports.create = createFilterDistinct;
