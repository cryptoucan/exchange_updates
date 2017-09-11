var config = require('./config.json')
var AWS = require('aws-sdk');
AWS.config.update({region: 'us-west-1'});

var ddb = new AWS.DynamoDB(config.api.aws);
var MAX_WRITE_PER_BATCH = 25

function batchWrite(requestsArray) {
	if (!requestsArray.length) {
		return
	}

	var request = requestsArray.pop()
	ddb.batchWriteItem(request, function(err, data) {
    	batchWrite(requestsArray)
	});
}

function scan(params, completion) {
	ddb.scan(params, function(err, data) {
		completion(err, data)
	});
}

module.exports = {
	batchWrite: batchWrite,
	scan: scan,
	MAX_WRITE_PER_BATCH: MAX_WRITE_PER_BATCH
}