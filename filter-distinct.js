var _       = require('lodash');
var q       = require('q');
var extend  = require('util')._extend;
var flatten = require('flat');
var levelup = require('levelup');
var varType = require('var-type');

var classify = require('./classify');

var Queue = require('promise-queue');
Queue.configure(q.Promise);
var maxConcurrent = 1;
var queue = new Queue(maxConcurrent);

var db;

function classifyProperties (data) {
	if(varType(data, 'Array')) {
		return data.map(function(dataItem) {
				return classifyProperties(data)
			}).concat(classify(dataItem));
	};

	if (varType(data, 'Object')) {
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
		var featureClass = featureProperties.map(function (propertyPath) {
				return _.get(propertyClasses, propertyPath);
			});
        return JSON.stringify(featureClass);
	});
}

//perform set of lookups and create keys for failed lookups
//resolve with true if any of the lookups from the set was failing, false if all lookups exist
function findOrCreate(db, group, lookupSet) {
    var lookupPromises = [];
    var lookupPromiseKeys = [];

    Object.keys(lookupSet).forEach(function (key) {
        var lookupKey = ('group:' + group + ', key: '+ key + ', class:' + lookupSet[key]);

        lookupPromises.push(q.ninvoke(db, 'get', lookupKey));
        lookupPromiseKeys.push(lookupKey);
    });

    return q.allSettled(lookupPromises)
        .then(function (results) {
            var batchOperations = [];

            results.forEach(function (result, index) {
                if (result.state === "rejected") {
                    batchOperations.push({type:'put', key: lookupPromiseKeys[index], value:true});
                }
            });

            if (batchOperations.length === 0) {
                //bone of the properties or features were distinct, report as not distinct
                return false
            }

            return q.ninvoke(db, 'batch', batchOperations)
            .then(function () {
                //some properties or features were distinct, report as distinct
                return true;
            })
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
        memdown.clearGlobalStore();

        db = levelup({ db: memdown });
    }

	return function (data) {
		//remove properties that have value 'undefined';
		data = JSON.parse(JSON.stringify(data));

	  	var group = groupExtractor(data);
	  	var extractedProperties = propertyExtractor(data);
	  	var propertyClasses = classifyProperties(extractedProperties);
	  	var featureClasses = classifyFeatures(propertyClasses, features);
	  	var lookupSet = flatten({
	  			properties: propertyClasses,
	  			features: featureClasses
	  	});

        return queue.add(function () {
            return findOrCreate(db, group, lookupSet)
        });
    };
}

module.exports.create = createFilterDistinct;
