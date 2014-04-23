var EventEmitter = require('events').EventEmitter;
var _ = require('underscore');
var Message = require('./Message');
var ProduceRequest = require('./ProduceRequest');
var Connection = require('./Connection');
var ConnectionPool = require('./ConnectionPool');

var Producer = function(topic, options){
  if (!topic || (!_.isString(topic))){
    throw "the first parameter, topic, is mandatory.";
  }
  this.MAX_MESSAGE_SIZE = 1024 * 1024; // 1 megabyte
  options = options || {};
  this.topic = topic;
  this.partition = options.partition || 0;
  this.host = options.host || 'localhost';
  this.port = options.port || 9092;
  this.useConnectionPool = options.connectionPool;

  this.connection = null;
};

Producer.prototype = Object.create(EventEmitter.prototype);

Producer.prototype.connect = function(){
  var that = this;
  if (this.useConnectionPool) {
      this.connection = Producer._connectionPool.getConnection(this.port, this.host);
  } else {
      this.connection = new Connection(this.port, this.host);
  }
  this.connection.once('connect', function(){
    that.emit('connect');
  });
  this.connection.once('error', function(err){
    that.emit('error', err);
  });
  this.connection.connect();
};

Producer.prototype.send = function(messages, options, cb) {
  var that = this;
  if (arguments.length === 2){
    // "options" is not a required parameter, so handle the
    // case when it's not set.
    cb = options;
    options = {};
  }
  if (!cb || (typeof cb != 'function')){
    throw "A callback with an error parameter must be supplied";
  }
  options.partition = options.partition || this.partition;
  options.topic = options.topic || this.topic;
  messages = toListOfMessages(toArray(messages));
  var request = new ProduceRequest(options.topic, options.partition, messages);

  this.connection.write(request.toBytes(), cb);
};

Producer._connectionPool = new ConnectionPool();

Producer.clearConnectionPool = function() {
    Producer._connectionPool.clear();
};

module.exports = Producer;

var toListOfMessages = function(args) {
  return _.map(args, function(arg) {
    if (arg instanceof Message) {
      return arg;
    }
    return new Message(arg);
  });
};

var toArray = function(arg) {
  if (_.isArray(arg))
    return arg;
  return [arg];
};
