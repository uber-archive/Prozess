var Connection = require('./Connection');
var net = require('net');

var ConnectionPool = function() {
    this.connections = {};
}

ConnectionPool.prototype.clear = function() {
    this.connections = {};
};

ConnectionPool.prototype.getConnection = function(port, host) {
    var connectionString = host + ':' + port;
    var connection = this.connections[connectionString];

    if (!connection) {
        connection = new Connection(port, host);
        this.connections[connectionString] = connection;
    }

    return connection;
};

module.exports = ConnectionPool;
