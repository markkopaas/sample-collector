var filterDistinctFactory 	= require('./filter-distinct');
var es 				= require('event-stream');
var request 		= require('request');
var url             = require('url');
var Logparser       = require('logagent-js');

var filterDistinctOptions = {
    preserve: false,
	//select properties that are used for determining distinct records
	propertyExtractor: function (data) {
		return {
			query: 	data.query
		};
	},

	//define sets of properties where it is important to track all combinations
	features: {x:['query.osc', 'query.author']},

	//each group will have its own reduced set of data
	groupExtractor: function (data) {
		return data.method + data.file;
    }
};

var filterDistinct = filterDistinctFactory.create(filterDistinctOptions);

var ins=0;
var outs=0;

function extractData(input) {
    return input && {
        method: input.method,
        status_code: input.status_code,
        path: input.path,
        file:   input.path && input.path.replace(url.parse(input.path).search,''),
        query:  input.path && url.parse(input.path, true).query
    }
}

function filterSkipEmptyLines(line) {
    if(line) {
        return line
    }
}

// es.readArray([{"method":"GET","status_code":200,"path":"/logs/access_151223.log","file":"/logs/access_151223.log","query":{}},
// {"method":"GET","status_code":200,"path":"/logs/access_151223.log","file":"/logs/access_151223.log","query":{}},
// {"method":"GET","status_code":200,"path":"/logs/access_151223.log","file":"/logs/access_151223.log","query":{}},])
//es.readArray([{a:1},{b:['2','3']},{d:{x:'3'}}])
// request("http://redlug.com/logs/access.log")
request("http://www.hoonlir.com/logs/access.log")
    //split stream to break on newlines
    .pipe(es.split())
    .on('data', function () {ins++;})
    .pipe(es.mapSync(filterSkipEmptyLines))
    .pipe(es.map(function(line, callback) {
        new Logparser().parseLine(line, '', callback)})
    )
    .pipe(es.mapSync(extractData))
    .pipe(es.map(function (data, callback) {
        filterDistinct(data, function (error, distinct) {
            if (error) {
                callback(error);
                return
            }

            if (distinct) {
                //distinct, keep
                callback(null, data);
                return;
            }
            //not distinct, skip
            callback();
        })
    }))
    .pipe(es.stringify())
    .on('data', function () {outs++;})
    .on('end', function (err) {
        console.log('DONE!');
        console.log('in:' + ins+' out:' + outs);
    })
 	.pipe(process.stdout)
