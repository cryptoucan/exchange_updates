var request = require('request');
var awsWrapper = require('./aws')
var updateMaker = require('./exchangeupdate')

module.exports = {
	getChanges: getChanges
}

function getChanges(callback) {
    var requestParams = {
    	url: 'https://api.bitfinex.com/v1/symbols'
    	
    }
	request(requestParams, function (error, response, body) {
		if (error || response.statusCode != 200) {
			callback([])
			return
		}

	  	handleGetLatestCoinsResponse(JSON.parse(body), callback)
	})
}

function handleGetLatestCoinsResponse(pairs, callback) {
	var params = {
  		TableName: 'bitfinex'
	};

	awsWrapper.scan(params, function(err, dbCoins) {
		if(err || !dbCoins) {
			callback([])
			return
		}

		comparePairsWithDB(pairs, dbCoins, function(update) {
			if (update.newCoins.length || update.minorUpdates.length) {
				updateDBWithPairs(pairs)
			}
			callback(update)
		})
	});
}

function comparePairsWithDB(pairs, dbPairs, completion) {
	var newCoins = []

	pairs = pairs.map(function(pair) {
		return formatPairString(pair)
	})
	
	for (var key in pairs) {
		var pair = pairs[key]
		
		var dbPair = findPair(pair, dbPairs.Items)

		if (dbPair == undefined) {
			newCoins.push(pair)
		} 
	}

	completion(updateMaker.createUpdate("Bitfinex", newCoins, []))
}

function formatPairString(pair) {
	var SYMBOL_LENGTH = 3
	return [pair.slice(0, SYMBOL_LENGTH), "_", pair.slice(SYMBOL_LENGTH)].join('').toUpperCase()
}

function findPair(pair, pairs) {
	for (var i = 0; i < pairs.length; i++) {
		if (pairs[i].pair.S.toUpperCase() === pair.toUpperCase()) {
			return pairs[i]
		}
	}
}

function emptyWriteData() {
	return {
		'RequestItems' : {
			'bitfinex' : []
		}
	}
}

function updateDBWithPairs(pairs) {
	var requests = []
	var currIndex = -1
	var count = 0
	for (var key in pairs) {
		if (count == undefined || count % awsWrapper.MAX_WRITE_PER_BATCH == 0) {
			requests.push(emptyWriteData())
			currIndex++
			count = 0
		}

		var pair = pairs[key]
		requests[currIndex].RequestItems.bitfinex.push({
			'PutRequest': {
				'Item': {
					'pair': {
						S: formatPairString(pair)
					},
					'lastModifiedDate': {
						S: Date()
					}
				}
			}
		})
		count++
	}

	awsWrapper.batchWrite(requests)
}