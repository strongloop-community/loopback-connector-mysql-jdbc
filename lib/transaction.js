var debug = require('debug')('loopback:connector:mysql:transaction');
module.exports = mixinTransaction;

/*!
 * @param {MySQLJava} MySQLJava connector class
 * @param {Object} mysql mysql driver
 */
function mixinTransaction(MySQLJava, java) {

  var Connection = java.import('java.sql.Connection');
  var isolationLevels = {
    'READ COMMITTED': Connection.TRANSACTION_READ_COMMITTED,
    'READ UNCOMMITTED': Connection.TRANSACTION_READ_UNCOMMITTED,
    'REPEATABLE READ': Connection.TRANSACTION_REPEATABLE_READ,
    'SERIALIZABLE': Connection.TRANSACTION_SERIALIZABLE
  };

  /**
   * Begin a new transaction
   * @param isolationLevel
   * @param cb
   */
  MySQLJava.prototype.beginTransaction = function(isolationLevel, cb) {
    debug('Begin a transaction with isolation level: %s', isolationLevel);
    this.pool.acquire(function(err, connection) {
      if (err) return cb(err);
      connection.setAutoCommit(false);
      if (isolationLevel) {
        connection.setTransactionIsolation(isolationLevels[isolationLevel]);
      }
      cb(null, connection);
    });
  };

  /**
   *
   * @param connection
   * @param cb
   */
  MySQLJava.prototype.commit = function(connection, cb) {
    var self = this;
    debug('Commit a transaction');
    connection.commitAsync(function(err) {
      connection.setAutoCommit(true);
      self.pool.release(connection);
      cb(err);
    });
  };

  /**
   *
   * @param connection
   * @param cb
   */
  MySQLJava.prototype.rollback = function(connection, cb) {
    var self = this;
    debug('Rollback a transaction');
    connection.rollbackAsync(function(err) {
      connection.setAutoCommit(true);
      self.pool.release(connection);
      cb(err);
    });
  };
}