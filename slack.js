var Slack = require('slack-node');
apiToken = "";
 
slack = new Slack(apiToken);
 
slack.api("groups.list", function(err, response) {
  console.log(response);
});
 
slack.api('chat.postMessage', {
  text:'test',
  channel:'',
  as_user: false,
  username: 'exchange_updates'
}, function(err, response){
  console.log(response);
});