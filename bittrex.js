var awsWrapper = require('./aws')
var updateMaker = require('./exchangeupdate')
var bittrex = require('node.bittrex.api');
var config = require('./config.json');
bittrex.options({
  'apikey' : config.api.bittrex.key,
  'apisecret' : config.api.bittrex.secret, 
});

module.exports = {
	getChanges: getChanges
}

function getChanges(callback) {
	bittrex.getcurrencies(function(data, err) {
		if (!err) {
			handleGetCoins(data.result, callback)
		}
	})
}

function handleGetCoins(latestCoins, callback) {
	if (!latestCoins) {
		callback([])
		return
	}

	var params = {
  		TableName: 'bittrex'
	};

	awsWrapper.scan(params, function(err, dbCoins) {
		if (err || !dbCoins) {
			callback([])
			return
		}

		compareCoinsWithDB(latestCoins, dbCoins, function(exchangeUpdate) {
			if (exchangeUpdate.newCoins.length || exchangeUpdate.minorUpdates.length) {
				updateDBWithCoins(latestCoins)
			}
			callback(exchangeUpdate)
		})
	});
}

function compareCoinsWithDB(latestCoins, dbCoins, completion) {
	var changes = []
	var majorChanges = []
	var minorChanges = []

	for (var key in latestCoins) {
		var coin = latestCoins[key]
		var dbCoin = findCoin(coin.Currency, dbCoins.Items)

		if (dbCoin == undefined) {
			majorChanges.push(coin.Currency)
		} else if (coin.IsActive != dbCoin.isActive.N) {
			var change = coin.Currency + " active status changed from " + (dbCoin.isActive.N == 1 ? "true" : "false") + " to " + coin.IsActive
			minorChanges.push(change)
		} 
	}

	completion(updateMaker.createUpdate("Bittrex", majorChanges, minorChanges))
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
			'bittrex' : []
		}
	}
}

function updateDBWithCoins(coins) {
	var requests = []
	var currIndex = -1
	var count = 0

	// we cant write too many at once so break it into chunks
	for (var key in coins) {
		if (count == 0 || count % awsWrapper.MAX_WRITE_PER_BATCH == 0) {
			requests.push(emptyWriteData())
			currIndex++
			count = 0
		}

		var coin = coins[key]

		requests[currIndex].RequestItems.bittrex.push({
			'PutRequest': {
				'Item': {
					'symbol':{
						S: coin.Currency
					},
					'isActive': {
						N: coin.IsActive ? "1" : "0"
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