var request = require('request');
var awsWrapper = require('./aws')
var updateMaker = require('./exchangeupdate')

module.exports = {
	getChanges: getChanges
}

function getChanges(callback) {
	request('https://shapeshift.io/getcoins', function (error, response, body) {
	  if (!error && response.statusCode == 200) {
	  	var json = JSON.parse(body)
	  	handleFetchNewCoinsResponse(json, callback)
	  }
	})
}

function handleFetchNewCoinsResponse(shapeshiftCoins, callback) {
	var params = {
  		TableName: 'shapeshift'
	};

	awsWrapper.scan(params, function(err, dbCoins) {
		if (err || !dbCoins) {
			callback()
			return
		}

		compareCoinsWithDB(shapeshiftCoins, dbCoins, function(update) {
			if (update.newCoins.length || update.minorUpdates.length) {
				updateDBWithCoins(shapeshiftCoins)
			}
			callback(update)
		})
	});
}

function compareCoinsWithDB(shapeshiftCoins, dbCoins, completion) {
	var newCoins = []
	var minorUpdates = []

	for (var key in shapeshiftCoins) {
		var shapeshiftCoin = shapeshiftCoins[key]
		var dbCoin = findCoin(shapeshiftCoin.symbol, dbCoins.Items)

		if (dbCoin == undefined) {
			newCoins.push(shapeshiftCoin.symbol)
		} else if (shapeshiftCoin.status !== dbCoin.status.S) {
			var change = shapeshiftCoin.symbol + " status changed from " + dbCoin.status.S + " to " + shapeshiftCoin.status
			minorUpdates.push(change)
		}
	}

	completion(updateMaker.createUpdate("Shapeshift", newCoins, minorUpdates))
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
			'shapeshift' : []
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
		requests[currIndex].RequestItems.shapeshift.push({
			'PutRequest': {
				'Item': {
					'symbol': {
						S: coin.symbol
					},
					'status': {
						S: coin.status
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