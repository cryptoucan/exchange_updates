var request = require('request');
var awsWrapper = require('./aws')
var updateMaker = require('./exchangeupdate')

module.exports = {
	getChanges: getChanges
}

function getChanges(callback) {
    var requestParams = {
    	url: 'https://www.binance.com/exchange/public/product'
    	
    }
	request(requestParams, function (error, response, body) {
		if (error || response.statusCode != 200) {
			callback([])
			return
		}
		let data = JSON.parse(body).data
	  	handleGetLatestCoinsResponse(data, callback)
	})
}

function handleGetLatestCoinsResponse(coinsArray, callback) {
	var params = {
  		TableName: 'binance'
	};

	awsWrapper.scan(params, function(err, dbCoins) {
		if(err || !dbCoins) {
			callback([])
			return
		}

		comparePairsWithDB(coinsArray, dbCoins, function(update) {
			if (update.newCoins.length || update.minorUpdates.length) {
				updateDBWithPairs(coinsArray)
			}
			callback(update)
		})
	});
}


function comparePairsWithDB(coinsArray, dbCoins, completion) {
	var newCoins = []

	for (var idx in coinsArray) {
		var coin = coinsArray[idx]
		var pair = coin.symbol
		
		var dbPair = findPair(pair, dbCoins.Items)

		if (dbPair == undefined) {
			newCoins.push(pair)
		}
	}

	completion(updateMaker.createUpdate("Binance", newCoins, []))
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
			'binance' : []
		}
	}
}

function updateDBWithPairs(coins) {
	var requests = []
	var currIndex = -1
	var count = 0
	for (var key in coins) {
		if (count == undefined || count % awsWrapper.MAX_WRITE_PER_BATCH == 0) {
			requests.push(emptyWriteData())
			currIndex++
			count = 0
		}

		var coin = coins[key]
		requests[currIndex].RequestItems.binance.push({
			'PutRequest': {
				'Item': {
					'symbol': {
						S: coin.quoteAsset
					},
					'pair': {
						S: coin.symbol
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