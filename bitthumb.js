var request = require('request');
var awsWrapper = require('./aws')
var updateMaker = require('./exchangeupdate')

module.exports = {
	getChanges: getChanges
}

function getChanges(callback) {
	request('https://api.bithumb.com/public/ticker/ALL', function (error, response, body) {
	  if (!error && response.statusCode == 200) {
	  	var json = JSON.parse(body)
	  	// not needed
	  	delete json.data['date']
	  	handleGetCoins(json.data, callback)
	  } else {
	  	callback([])
	  }
	})
}

function handleGetCoins(coins, callback) {
	if (!coins) {
		callback([])
		return
	}

	var params = {
  		TableName: 'bitthumb'
	};

	awsWrapper.scan(params, function(err, dbCoins) {
		if (err || !dbCoins) {
			callback([])
			return
		}

		compareCoinsWithDB(coins, dbCoins, function(update) {
			if (update.newCoins.length || update.minorUpdates.length) {
				updateDBWithCoins(coins)
			}
			callback(update)
		})
	});
}

function compareCoinsWithDB(coins, dbCoins, completion) {
	var newCoins = []

	for (var key in coins) {
		var coin = coins[key]
		var dbCoin = findCoin(key, dbCoins.Items)

		if (dbCoin == undefined) {
			newCoins.push(key)
		} 
	}

	completion(updateMaker.createUpdate("Bitthumb", newCoins, []))
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
			'bitthumb' : []
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
		requests[currIndex].RequestItems.bitthumb.push({
			'PutRequest': {
				'Item': {
					'symbol': {
						S: key
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