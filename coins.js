var poloniex = require('./poloniex')
var shapeshift = require('./shapeshift')
var binance = require('./binance')
var bittrex = require('./bittrex')
var bitthumb = require('./bitthumb')
var coinone = require('./coinone')
var bitfinex = require('./bitfinex')
var config = require('./config.json')
var Slack = require('slack-node');
slack = new Slack(config.api.slack.token);

var nodemailer = require('nodemailer');
const Nexmo = require('nexmo');
const nexmo = new Nexmo({
  apiKey: config.api.nexmo.key,
  apiSecret: config.api.nexmo.secret
});

exports.handler = function(event, context, callback) {
	execute()
}

function execute() {
	poloniex.getChanges(function(update) {
		notifyChanges(update)
	})

	binance.getChanges(function(update) {
		notifyChanges(update)
	})

	shapeshift.getChanges(function(update) {
		notifyChanges(update)
	})

	bitthumb.getChanges(function(update) {
		notifyChanges(update)
	})

	coinone.getChanges(function(update) {
		notifyChanges(update)
	})

	bitfinex.getChanges(function(update) {
		notifyChanges(update)
	})

	bittrex.getChanges(function(update) {
		notifyChanges(update)
	})
}

function notifyChanges(update) {
	var message = ""
	if (update.newCoins.length) {
		message = update.exchangeName + " added: "
		var firstUpdate = true
		for (var i = 0; i < update.newCoins.length; i++) {
			message = message + (!firstUpdate ? ", " : " ") + update.newCoins[i]
			if (firstUpdate) {
				firstUpdate = false
			}
		}
	}

	if (message) {
		postToDogPound(message)
		sendSMS(message)
	}

	// sendSMS(message)
}

function postToDogPound(message) {
	slack.api('chat.postMessage', {
	  text:message,
	  channel:'G70RS3GVB',
	  as_user: false,
	  username: 'exchange_updates'
	}, function(err, response){
	
	});
}

function sendSMS(message) {
	nexmo.message.sendSms(
 			    config.api.nexmo.number, config.contact.mobile, message,
				    (err, responseData) => {
				      if (err) {
				        console.log(err);
				      } else {
				        console.dir(responseData);
				      }
				    }
 				);
}

function sendEmail(message) {
	var transporter = nodemailer.createTransport({
	  service: 'gmail',
		  auth: {
		    user: config.contact.email.address,
		    pass: config.contact.email.password
		  }
		});
	var mailOptions = {
	  from: config.contact.email.address,
	  to: config.contact.email.address,
	  subject: 'Coin updates',
	  text: message
	};

	transporter.sendMail(mailOptions, function(error, info){
		if (error) {
    		console.log(error);
  		} else {
   			 console.log('Email sent: ' + info.response);
 		 }
	});

}