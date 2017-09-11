function createUpdate(exchangeName, newCoins, minorUpdates) {
	return {
		"exchangeName": exchangeName,
		"newCoins": newCoins,
		"minorUpdates": minorUpdates
	}
}

module.exports = {
	createUpdate: createUpdate
}