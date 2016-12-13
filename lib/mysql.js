var MySQL = require('loopback-connector-mysql').MySQL;
var debug = require('debug')('loopback:connector:mysql-jdbc');
var poolModule = require('generic-pool');

var path = require('path');
var java = require('java');

java.asyncOptions = {
  asyncSuffix: 'Async', // generate methodAsync()
  syncSuffix: ''        //  methods use the base name(!!)
};

java.classpath.push(path.join(__dirname,
  '../java-libs/mysql-connector-java-5.1.35-bin.jar'));

/*
 java.ensureJvm(function(err, flag) {
 console.log(err, flag);
 });
 */

java.import('java.sql.Connection');
var DriverManager = java.import('java.sql.DriverManager');
java.import('java.sql.SQLException');

var SqlTypes = java.import('java.sql.Types');
var Statement = java.import('java.sql.Statement');

/**
 * @module loopback-connector-mysql
 *
 * Initialize the MySQL connector against the given data source
 *
 * @param {DataSource} dataSource The loopback-datasource-juggler dataSource
 * @param {Function} [callback] The callback function
 */
exports.initialize = function initializeDataSource(dataSource, callback) {
  dataSource.connector = new MySQLJava(dataSource.settings);
  dataSource.connector.dataSource = dataSource;

  dataSource.connector.connect(callback);
};

/**
 * @constructor
 * Constructor for MySQL connector
 * @param {Object} client The node-mysql client object
 */
function MySQLJava(settings) {
  this.name = 'mysql';
  this.settings = settings || {};
  this._models = {};
}

require('util').inherits(MySQLJava, MySQL);

MySQLJava.prototype.getConnection = function(cb) {
  var s = this.settings;
  var url = s.url;
  if (!url) {
    var host = s.host || s.hostname || 'localhost';
    var port = s.port || 3306;
    var user = s.username || s.user;
    var password = s.password;
    url = 'jdbc:mysql://' + host + ':' + port + '/' + s.database +
      '?user=' + user + '&password=' + password;
  }
  DriverManager.getConnectionAsync(url, cb);
}

MySQLJava.prototype.connect = function(cb) {
  var self = this;
  if (!this.pool) {
    this.pool = poolModule.Pool({
      name: 'mysql',
      create: function(callback) {
        self.getConnection(callback);
      },
      destroy: function(connection) {
        connection.close();
      },
      max: 10,
      // optional. if you set this, make sure to drain() (see step 3)
      min: 2,
      // specifies how long a resource can stay idle in pool before being removed
      idleTimeoutMillis: 30000
    });
  }
  process.nextTick(cb);
};

MySQLJava.prototype.escapeName = function(name) {
  return '`' + name + '`';
  // return name;
};

function buildRowsFromResultSet(rs) {
  var rows = [];
  if (!rs) return rows;
  var meta = rs.getMetaData();
  var colCount = meta.getColumnCount();
  var colNames = [''];
  for (var i = 1; i <= colCount; i++) {
    colNames.push(meta.getColumnLabel(i));
  }
  while (rs.next()) {
    var row = {};
    for (i = 1; i <= colCount; i++) {
      var colType = meta.getColumnType(i);

      if (colType === SqlTypes.ARRAY) {
        row[colNames[i]] = rs.getArray(i);
      }
      else if (colType === SqlTypes.BIGINT) {
        row[colNames[i]] = rs.getInt(i);
      }
      else if (colType === SqlTypes.BOOLEAN) {
        row[colNames[i]] = rs.getBoolean(i);
      }
      else if (colType === SqlTypes.BLOB) {
        row[colNames[i]] = rs.getBlob(i);
      }
      else if (colType === SqlTypes.DOUBLE) {
        row[colNames[i]] = rs.getDouble(i);
      }
      else if (colType === SqlTypes.FLOAT) {
        row[colNames[i]] = rs.getFloat(i);
      }
      else if (colType === SqlTypes.INTEGER) {
        row[colNames[i]] = rs.getInt(i);
      }
      else if (colType === SqlTypes.NVARCHAR) {
        row[colNames[i]] = rs.getNString(i);
      }
      else if (colType === SqlTypes.VARCHAR) {
        row[colNames[i]] = rs.getString(i);
      }
      else if (colType === SqlTypes.TINYINT) {
        row[colNames[i]] = rs.getInt(i);
      }
      else if (colType === SqlTypes.SMALLINT) {
        row[colNames[i]] = rs.getInt(i);
      }
      else if (colType === SqlTypes.DATE) {
        row[colNames[i]] = rs.getDate(i);
      }
      else if (colType === SqlTypes.TIMESTAMP) {
        row[colNames[i]] = rs.getTimestamp(i);
      }
      else {
        row[colNames[i]] = rs.getObject(i);
      }
    }
    debug('Data: %j', row);
    rows.push(row);
  }
  return rows;
}
/**
 * Execute the sql statement
 *
 * @param {String} sql The SQL statement
 * @param {Function} [callback] The callback after the SQL statement is executed
 */
MySQLJava.prototype.executeSQL = function(sql, params, options, cb) {
  var self = this;
  debug('SQL: %s Parameters: %j', sql, params);

  function executeWithConnection(conn, inTx) {
    conn.prepareStatementAsync(sql, Statement.RETURN_GENERATED_KEYS,
      function(err, stmt) {
        for (var i = 0, n = params.length; i < n; i++) {
          var p = params[i];
          var index = i + 1;
          if (p == null) {
            stmt.setNull(index);
          } else {
            switch (typeof p) {
              case 'string':
                stmt.setString(index, p);
                break;
              case 'number':
                if (p.toString().indexOf('.') !== -1) {
                  stmt.setDouble(index, p)
                } else {
                  stmt.setInt(index, p);
                }
                break;
              case 'boolean':
                stmt.setBoolean(index, p);
                break;
              case 'object':
                if (Array.isArray(p)) {
                  stmt.setArray(index, p);
                } else {
                  stmt.setObject(index, p);
                }
                break;
              default:
                stmt.setObject(index, p);
            }
          }
        }
        stmt.executeAsync(function(err, isResultSet) {
          if (err) return cb(err);
          var rs;
          var rows;
          if (isResultSet) {
            debug('ResultSet is returned');
            rs = stmt.getResultSet();
            rows = buildRowsFromResultSet(rs);
            stmt.close();
            if (!inTx) {
              self.pool.release(conn);
            }
            cb(null, rows);
          } else {
            var count = stmt.getUpdateCount();
            debug('count: %d', count);
            rs = stmt.getGeneratedKeys();
            rows = buildRowsFromResultSet(rs);
            debug('Rows: %j', rows);
            stmt.close();
            var id = rows[0] && rows[0].GENERATED_KEY;
            var result = {affectedRows: count};
            if (id != null) {
              result.insertId = id;
            }
            if (!inTx) {
              self.pool.release(conn);
            }
            return cb(err, result);
          }
        });
      });
  }

  var transaction = options.transaction;
  if (transaction && transaction.connection &&
    transaction.connector === this) {
    debug('Execute SQL within a transaction');
    executeWithConnection(transaction.connection, true);
  } else {
    debug('Execute SQL with a new connection');
    self.pool.acquire(function(err, conn) {
      if (err) return cb(err);
      executeWithConnection(conn, false);
    });
  }
};

/**
 * Disconnect from MySQL
 */
MySQLJava.prototype.disconnect = function(cb) {
  if (this.debug) {
    debug('disconnect');
  }
  this.pool.destroyAllNow();
  process.nextTick(cb);
};

require('./transaction')(MySQLJava, java);

