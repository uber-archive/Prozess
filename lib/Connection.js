var EventEmitter = require('events').EventEmitter;
var net = require('net');

var Connection = function(port, host) {
    this.port = port;
    this.host = host;

    this.connecting = false;
    this.connected = false;
    this._connection = null;
}

Connection.prototype = Object.create(EventEmitter.prototype);

Connection.prototype.connect = function() {
    var that = this;

    if (this.connected) {
        this.emit('connect');
        return;
    }
    if (this.connecting) {
        return;
    }

    this.connecting = true;

    this._connection = net.createConnection(this.port, this.host);
    this._connection.setKeepAlive(true, 1000);

    this._connection.once('connect', function() {
        that.connecting = false;
        that.connected = true;
        that.emit('connect');
    });

    this._connection.once('error', function(err) {
        that.connecting = false;

        if (!!err.message && err.message === 'connect ECONNREFUSED') {
            that.emit('error', err);
        }
    });
};

Connection.prototype._reconnect = function(callback) {
    var that = this;

    var onConnect = function() {
        that.removeListener('brokerReconnectError', onReconnectError);
        callback();
    };

    var onReconnectError = function() {
        that.removeListener('connect', onConnect);
        callback('brokerReconnectError');
    };

    this.once('connect', onConnect);
    this.once('brokerReconnectError', onReconnectError);

    if (this.connecting) return;

    this.connect();

    this._connection.on('error', function(err) {
        if (!!err.message && err.message === 'connect ECONNREFUSED') {
            that.emit('brokerReconnectError', err);
        } else {
            callback(err);
        }
    });
};

Connection.prototype.write = function(data, callback) {
    var that = this;

    this._connection.write(data, function(err) {
        if (!!err && err.message === 'This socket is closed.') {
            that.connected = false;
            that._reconnect(function(err) {
                if (err) {
                    return callback(err);
                }

                that._connection.write(data, function(err) {
                    if (!!err) {
                        that.connected = false;
                    }
                    return callback(err);
                });
            });
        } else {
            callback(err);
        }
    });
};

module.exports = Connection;
