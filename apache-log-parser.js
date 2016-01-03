var extend 	= require('util')._extend;
var url  	= require('url');

var apacheDate2isoDate = function (apacheDate) {
	if (typeof apacheDate === 'string') {
        //apache log time format: [DD/MMM/YYYY:HH:mm:ss ZZ]
		//if date and time are separated with space, javascript is able to parse it directly
		return new Date(apacheDate.replace('[','').replace(']','').replace(':', ' ')).toISOString()
	}
};

var defaultTransforms = {
		ip: 	0, 
		path: 	6, 
		status: function (dataset) {return dataset[8] && Number(dataset[8]);}, 
		size: 	function (dataset) {return dataset[9] && Number(dataset[9]);},
		date: 	function (dataset) {return dataset[3] && dataset[4] && apacheDate2isoDate(dataset[3] + ' ' + dataset[4]);}, 
		method: function (dataset) {return dataset[5] && dataset[5].replace('"','');},
		file: 	function (dataset) {return dataset[6] && url.parse(dataset[6]).path.replace(url.parse(dataset[6]).search,'');},
		query: 	function (dataset) {return dataset[6] && url.parse(dataset[6], true).query;},
}

var apacheLogParser = function (options) {
	transforms = extend(extend({}, defaultTransforms), options && options.transforms || {})
	
	return function (line, callback) {
		var dataset = line.split(" ");
		var result = {};

        Object.keys(transforms).forEach(function (key) {
			if (typeof transforms[key] === 'function') {
				return transforms[key](dataset);
			}
			
			return dataset[transforms[key]];
		});

	  	callback(null, result);
	};
};

module.exports = apacheLogParser;