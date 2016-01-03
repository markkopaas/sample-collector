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
	features: {x:['query.author', 'query.author']},

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

function filterLines(line) {
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
    .on('end', function () {console.log('end1')})
    // .pipe(es.mapSync(filterLines))
    .pipe(es.through(
        function write(data) {
            if(data) {
                this.emit('data', data)
            }
            //this.pause()
        },
        function end () { //optional
            this.emit('end')
        }).on('error', function (err) {console.log(err)}).on('end', function () {console.log('end2')}))
.on('end', function () {console.log('end2,5')})
    .pipe(es.map(function(line, callback) {
        new Logparser().parseLine(line, '', callback)})
    )
    .on('end', function () {console.log('end3')})

    .pipe(es.mapSync(extractData))
    .on('end', function () {console.log('end3,5')})
    .pipe(es.map(function (data, callback) {
        filterDistinct(data, function (error, distinct) {
            if (error) {
                callback(error);
                return
            }

            if (distinct) {
                console.log('distinct')
                callback(null, data);
                return;
            }
            console.log('not distinct')
            //skip
            callback();
        })
    }))
    .on('error', console.log)
    .on('end', function () {console.log('end4')})
    .pipe(
        es.mapSync(function(data) {
            outs++;
            return data;
        })
    )
    .on('end', function () {console.log('end5')})
    .pipe(es.stringify())
    .on('end', function () {console.log('end6')})
    .on('end', function (err) {
        console.log('DONE!');
        console.log('in:' + ins+' out:' + outs);
    })
 	.pipe(process.stdout)
