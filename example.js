var es 				= require('event-stream');
var request 		= require('request');
var url             = require('url');
var varType         = require('var-type');
var serialize       = require('serialize');

var Logparser       = require('logagent-js');
var logParser = new Logparser();
var parseLine = serialize(logParser.parseLine.bind(logParser));

var filterDistinctFactory 	= require('./filter-distinct');
var filterDistinctOptions = {
        preserve: true,
        //select properties that are used for determining distinct records
        propertyExtractor: function (data) {
            return {
                query: 	data.query
            };
        },

        //define sets of dependent properties where it is important to track combinations of value classes
        features: {x:['query.osc', 'query.author']},

        //similar property sets are treated distinct, if in different groups
        groupExtractor: function (data) {
            return data.method + data.file;
        }
    };

var filterDistinct = filterDistinctFactory.create(filterDistinctOptions);

var ins=0;
var outs=0;

function extractData (input) {
    if (varType(input, 'Object')) {
        return {
            method: input.method,
            status_code: input.status_code,
            path: input.path,
            file:   input.path && input.path.replace(url.parse(input.path).search,''),
            query:  input.path && url.parse(input.path, true).query
        }
    }
}

function filterSkipEmptyLines(line) {
    if (line) {
        return line
    }
}

// request("http://redlug.com/logs/access.log")
request("http://www.hoonlir.com/logs/access.log")
    //split stream to break on newlines
    .pipe(es.split())
    .on('data', function () {ins++;})
    .pipe(es.mapSync(filterSkipEmptyLines))
    .pipe(es.map(function(line, callback) {
        parseLine(line, '', callback)})
    )
    .pipe(es.mapSync(extractData))
    .pipe(es.map(function (data, callback) {
        filterDistinct(data)
        .then(function (distinct) {
            if (distinct) {
                //distinct, keep
                callback(null, data);
                return;
            }
            //not distinct, skip
            callback();
        })
        .catch(callback);
    }))
    .pipe(es.stringify())
    .on('data', function () {outs++;})
    .on('end', function (err) {
        console.log('DONE!');
        console.log('in:' + ins+' out:' + outs);
    })
 	.pipe(process.stdout)
