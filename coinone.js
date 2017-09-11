var request = require('request');
var awsWrapper = require('./aws')
var updateMaker = require('./exchangeupdate')

module.exports = {
	getChanges: getChanges
}

function getChanges(callback) {
    var requestParams = {
    	url: 'https://api.coinone.co.kr/ticker/?currency=all&format=json',
    	timeout: 8000
    }
	request(requestParams, function (error, response, body) {
		if (error || response.statusCode != 200) {
			callback([])
			return
		}

	  	var json = JSON.parse(body)

	  	// clear 'date' and not needed fields (anything not an object is not a coin model)
	  	for (key in json) {
	  		if (typeof json[key] != 'object') {
	  			delete json[key]
	  		}
	  	}
	  	handleFetchNewCoinsResponse(json, callback)
	})
}

function handleFetchNewCoinsResponse(coins, callback) {
	var params = {
  		TableName: 'coinone'
	};
	awsWrapper.scan(params, function(err, dbCoins) {
		if (err || !dbCoins) {
			callback([])
			return
		}

		compareCoinsWithDB(coins, dbCoins, function(exchangeUpdate) {
			if (exchangeUpdate.newCoins.length || exchangeUpdate.minorUpdates.length) {
				updateDBWithCoins(coins)
			}
			callback(exchangeUpdate)
		})
	})
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

	completion(updateMaker.createUpdate("Coinone", newCoins, []))
}

function findCoin(symbol, coins) {
	for (var i = 0; i < coins.length; i++) {
		if (coins[i].symbol.S.toUpperCase() === symbol.toUpperCase()) {
			return coins[i]
		}
	}
}

function emptyWriteData() {
	return {
		'RequestItems' : {
			'coinone' : []
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
		requests[currIndex].RequestItems.coinone.push({
			'PutRequest': {
				'Item': {
					'symbol': {
						S: key.toUpperCase()
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