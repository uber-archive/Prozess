var Connection = require('./Connection');
var net = require('net');

var ConnectionPool = function() {
    this.connections = {};
}

ConnectionPool.prototype.clear = function() {
    delete this.connections;

    this.connections = {};
};

ConnectionPool.prototype.getConnection = function(port, host) {
    var connectionString = host + ':' + port;
    var connection = this.connections[connectionString];

    if (!connection) {
        connection = new Connection(port, host);
        connection.connect();

        this.connections[connectionString] = connection;
    }

    return connection;
};

module.exports = ConnectionPool;
