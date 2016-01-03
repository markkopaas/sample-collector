var _ = require('lodash');
var extend = require('util')._extend;
var varType = require('var-type');
var classifiers = {};

classifiers.default = {
    type: varType
};

classifiers.String = {
    lengthClass: function (data) {
        return data.length > 0 && Math.floor(Math.log(data.length) / Math.LN10) + 1;
    }
};

classifiers.Number = {
    isZero: function (data) {
        return data === 0;
    },
    sign: Math.sign,
    isInteger: function (data) {
        Math.floor(data) === data;
    },
    length: function (data) {
        return data > 0 && Math.floor(Math.log(Math.abs(data)) / Math.LN10) + 1;
    }
};

classifiers.Object = {
    propertyCount: function (data) {
        return Object.keys(data).length;
    }
};

classifiers.Array = {
    isEmptyArray: function (data) {
        return data.length === 0;
    },
    lengthClass: function (data) {
        return data.length > 0 && Math.floor(Math.log(data.length) / Math.LN10) + 1;
    }
};

var classify = function (data) {
    var predicateSet = extend(extend({}, classifiers.default), classifiers[varType(data)]);
	var predicatesResult = _.mapValues(predicateSet, function (predicate) {
		return predicate.call(null, data);
	})

	return JSON.stringify(predicatesResult);
}

module.exports = classify;
