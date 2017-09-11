var awsWrapper = require('./aws')
var updateMaker = require('./exchangeupdate')
var request = require('request');
const poloniexApi = require('poloniex-api-node')
var config = require('./config.json')
var poloniex = new poloniexApi(config.api.poloniex.key, config.api.poloniex.secret)

module.exports = {
	getChanges: getChanges
}

function getChanges(callback) {
	poloniex.returnCurrencies(function(err, data) {
		handleFetchNewCoinsResponse(data, callback)
	})
}

function handleFetchNewCoinsResponse(latestCoins, callback) {
	// get coins errored
	if (!latestCoins) {
		callback()
		return
	}

	var params = {
  		TableName: 'poloniex'
	};

	// scan db and compare with latestCoins
	awsWrapper.scan(params, function(err, dbCoins) {
		// db request errored, finish with no changes
		if (err || !dbCoins) {
			callback()
			return
		}

		compareCoinsWithDB(latestCoins, dbCoins, function(update) {	
			if (update.newCoins.length || update.minorUpdates.length) {
				updateDBWithCoins(latestCoins)	
			}		
			callback(update)
		})
	})
}

function compareCoinsWithDB(latestCoins, dbCoins, completion) {
	var newCoins = []
	var minorUpdates = []

	for (var key in latestCoins) {
		var coin = latestCoins[key]
		var dbCoin = findCoin(key, dbCoins.Items)

		if (dbCoin == undefined) {
			newCoins.push(key)
		} else if (coin.disabled != dbCoin.disabled.N) {
			var change = key + " enabled status changed from " + dbCoin.disabled.N + " to " + coin.disabled
			minorUpdates.push(change)
		} else if (coin.delisted != dbCoin.delisted.N) {
			var change = key + " delisted status changed from " + dbCoin.disabled.N + " to " + coin.disabled
			minorUpdates.push(change)
		} 
	}

	completion(updateMaker.createUpdate("Poloniex", newCoins, minorUpdates))
}

function findCoin(symbol, coins) {
	for (var i = 0; i < coins.length; i++) {
		if (coins[i].symbol.S === symbol) {
			return coins[i]
		}
	}
}

function emptyWriteData() {
	return {
		'RequestItems' : {
			'poloniex' : []
		}
	}
}

function updateDBWithCoins(coins) {
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

		requests[currIndex].RequestItems.poloniex.push({
			'PutRequest': {
				'Item': {
					'symbol':{
						S: key
					},
					'disabled': {
						N: coin.disabled.toString()
					},
					'delisted': {
						N: coin.delisted.toString()
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