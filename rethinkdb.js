var rethinkdbdash = require('rethinkdbdash');
var q = require('q'); // Can be easily removed
var assert = require('assert');
var async = require('async');
var utils = require('./lib/utils');
var mongoToReQL = require('./lib/mongo-to-reql');

require('protolog')();

/* There are two ways to instantiate a livedb-rethinkdb wrapper.
 *
 * 1. The simplest way is to just invoke the module and pass in your rethinkdbdash
 * arguments as arguments to the module function. For example:
 *
 * var db = require('livedb-rethinkdb')({ host: 'localhost', port: 28015, db: 'sharejs' });
 *
 * 2. If you already have a rethinkdbdash instance that you want to use, you can
 * just pass it into livedb-rethinkdb:
 *
 * var r = require('rethinkdbdash')({ host: 'localhost', port: 28015, db: 'sharejs' });
 * var db = require('livedb-rethinkdb')(r);
 */
exports = module.exports = function(r, options) {
  if (r.constructor !== rethinkdbdash().constructor) {
    r = rethinkdbdash(r);
  }
  return new liveDBRethinkDB(r, options);
};

// Deprecated. Don't use directly.
exports.liveDBRethinkDB = liveDBRethinkDB;

// r is an instance of rethinkdbdash. Create with:
// rethinkdbdash({ host: 'localhost', port: 28015, db: 'sharejs' })
function liveDBRethinkDB(r, options) {
  this.r = r;
  this.closed = false;

  if (!options) options = {};

  this.mongoPoll = options.mongoPoll || null;

  // The getVersion() and getOps() methods depend on a collectionname_ops
  // collection, and that collection should have an index on the operations
  // stored there. I could ask people to make these indexes themselves, but
  // even I forgot on some of my collections, so the mongo driver will just do
  // it manually. This approach will leak memory relative to the number of
  // collections you have, but if you've got thousands of mongo collections
  // you're probably doing something wrong.

  // map from collection name -> true for op collections we've ensureIndex'ed
  this.opIndexes = {};

  // Allow $while and $mapReduce queries. These queries let you run arbitrary
  // JS on the server. If users make these queries from the browser, there's
  // security issues.
  this.allowJSQueries = options.allowAllQueries || options.allowJSQueries || options.allowJavaScriptQuery || false;

  // Aggregate queries are less dangerous, but you can use them to access any
  // data in the mongo database.
  this.allowAggregateQueries = options.allowAllQueries || options.allowAggregateQueries;
}

liveDBRethinkDB.prototype.close = function(callback) {
  // There doesn't seem to be the need to close a connection
  if (this.closed) return callback('db already closed');
  callback(null, null);
  this.closed = true;
};

liveDBRethinkDB.prototype._check = function(cName) {
  if (this.closed) return 'db already closed';
  if (!utils.isValidCName(cName)) return 'Invalid collection name';
};

// **** Snapshot methods

liveDBRethinkDB.prototype.getSnapshot = function(cName, docName, callback) {
  var err; if (err = this._check(cName)) return callback(err);
  this.r.table(cName).get(docName)
    .run()
    .then(function (doc) {
      callback(null, utils.castToSnapshot(doc));
    })
    .catch(callback);
};

// Variant on getSnapshot (above) which projects the returned document
liveDBRethinkDB.prototype.getSnapshotProjected = function(cName, docName, fields, callback) {
  var err; if (err = this._check(cName)) return callback(err);

  // This code depends on the document being stored in the efficient way (which is to say, we've
  // promoted all fields in mongo). This will only work properly for json documents - which happen
  // to be the only types that we really want projections for.
  var projection = utils.projectionFromFields(fields);
  this.r.table(cName).get(docName)
    .pluck(projection)
    .run()
    .then(function (doc) {
      callback(null, utils.castToSnapshot(doc));
    })
    .catch(callback);
};

liveDBRethinkDB.prototype.bulkGetSnapshot = function(requests, callback) {
  if (this.closed) return callback('db already closed');

  var r = this.r;
  var results = {};

  var getSnapshots = function(cName, callback) {
    if (!utils.isValidCName(cName)) return 'Invalid collection name';

    var cResult = results[cName] = {};

    var docNames = requests[cName];
    r
      .table(cName)
      .filter(function (row) { return r.expr(docNames).contains(row('id')); })
      .run()
      .then(function (docs) {
        data = data && data.map(utils.castToSnapshot);

        for (var i = 0; i < data.length; i++) {
          cResult[data[i].docName] = data[i];
        }
        callback();
      })
      .catch(callback);
  };

  async.each(Object.keys(requests), getSnapshots, function(err) {
    callback(err, err ? null : results);
  });
};

liveDBRethinkDB.prototype.writeSnapshot = function(cName, docName, data, callback) {
  var err; if (err = this._check(cName)) return callback(err);
  var doc = utils.castToDoc(docName, data);
  this.r
    .table(cName)
    .insert(doc, {conflict: "update"})
    .run()
    .then(function (data) {
      callback(null, null);
    })
    .catch(callback);
};


// ******* Oplog methods

// Overwrite me if you want to change this behaviour.
liveDBRethinkDB.prototype.getOplogCollectionName = function(cName) {
  // Using an underscore to make it easier to see whats going in on the shell
  return cName + '_ops';
};

// Get and return the op table from rethinkdb, ensuring it has the op index.
liveDBRethinkDB.prototype._opCollection = function(cName) {
  var collection = this.r.table(this.getOplogCollectionName(cName));

  if (!this.opIndexes[cName]) {
    // Create index
    return this.r
      .tableCreate(this.getOplogCollectionName(cName))
      .run()
      .catch(function () {})
      .then(function () {
        return this.r
          .tableCreate(cName)
          .run()
          .catch(function () {});
      }.bind(this))
      .then(function () {
        return collection.indexCreate('name').run().catch(function () {});
      })
      .catch()// TODO: Add better error handling
      .then(function () {
        return collection.indexCreate('v').run().catch(function () {});
      }.bind(this))
      .catch(function (error) {
        console.warn('Warning: Could not create index for op collection:', error);
      })
      .then(function () {
        this.opIndexes[cName] = true;
        return collection;
      }.bind(this));
  }
  return q().then(function () {
    return collection;
  });
};

liveDBRethinkDB.prototype.writeOp = function(cName, docName, opData, callback) {
  assert(opData.v != null);

  var err; if (err = this._check(cName)) return callback(err);
  var self = this;

  var data = utils.shallowClone(opData);
  data.id = docName + ' v' + opData.v,
  data.name = docName;

  this._opCollection(cName)
    .then(function (collection) {
      collection
        .insert(data, {'conflict': 'update'})
        .run()
        .then(function (data) {
          calback(null, data);
        })
        .catch(callback);
    });
};

liveDBRethinkDB.prototype.getVersion = function(cName, docName, callback) {
  var err; if (err = this._check(cName)) return callback(err);

  var self = this;
  this._opCollection(cName)
    .then(function (collection) {
      collection
        .get(docName)
        .run()
        .then(function (data) {
          if (data === null) {
            self.r
              .table(cName)
              .get(docName)
              .pluck('_v') // When is it _v and when is it v?
              .run()
              .then(function(doc) {
                callback(null, doc ? doc._v : 0);
              })
              .catch(callback);
          } else {
            callback(err, data.v + 1);
          }
        })
        .catch(callback);
    });
};

liveDBRethinkDB.prototype.getOps = function(cName, docName, start, end, callback) {
  var err; if (err = this._check(cName)) return callback(err);
  var gt = function (row) {
    return row('v').ge(start);
  };
  var gtAndLt = function (row) {
    return row('v').ge(start).and(row('v').lt(end));
  };
  var query = (end == null ? gt : gtAndLt);
  this._opCollection(cName)
    .then(function (collection) {
      collection
        .orderBy({'index': 'v'}) // {sort:{v:1}}
        .filter({ name:docName }) // find({name: docName})
        .filter(query) // find({v: {$gte: start} || {$gte: start, $lt: end}})
        .run()
        .then(function (data) {
          for (var i = 0; i < data.length; i++) {
            // Strip out _id in the results
            delete data[i].id;
            delete data[i].name;
          }
          callback(null, data);
        })
        .catch(callback);
    });
};


// ***** Query methods

// Internal method to actually run the query.
liveDBRethinkDB.prototype._query = function(r, cName, query, fields, callback) {
  console.log('query', query);
  // For count queries, don't run the find() at all. We also ignore the projection, since its not
  // relevant.
  if (query.$count) {
    delete query.$count;
    r.table(cName)
      .filter(query.$query || true) // This won't work, since it's intended for Mongo
      .count()
      .run()
      .then(function (count) {
        callback(null, {results:[], extra: count});
      })
      .catch(callback);
  } else if (query.$distinct) {
    delete query.$distinct;
    // r.table(cName)

    mongo.collection(cName).distinct(query.$field, query.$query || {}, function(err, result) {
      if (err) return callback(err);
      callback(err, {results:[], extra:result});
    });

  } else if (query.$aggregate) {
    mongo.collection(cName).aggregate(query.$aggregate, function(err, result) {
      if (err) return callback(err);
      callback(err, {results:[], extra:result});
    });

  } else if (query.$mapReduce) {
    delete query.$mapReduce;

    var opt = {
      query: query.$query || {},
      out: {inline: 1},
      scope: query.$scope || {}
    };

    mongo.collection(cName).mapReduce(query.$map, query.$reduce, opt, function (err, mapReduce) {
      if (err) return callback(err);
      callback(err, {results: [], extra: mapReduce});
    });

  } else {
    var cursorMethods = utils.extractCursorMethods(query);

    // Weirdly, if the requested projection is empty, we send everything.
    var projection = fields ? utils.projectionFromFields(fields) : false;

    var reqlQuery = mongoToReQL(
      r.table(cName),
      query.$query
    );

    'reqlQuery'.log();
    reqlQuery.toString().log();

    // if (projection) reqlQuery reqlQuery.pluck(projection);

    reqlQuery.run().then(function (results) {
      results = results && results.map(utils.castToSnapshot);
      results.log();
      callback(null, results);
    })
    .catch(callback);

    // mongo.collection(cName).find(query, projection, function(err, cursor) {
    //   if (err) return callback(err);
    //
    //   for (var i = 0; i < cursorMethods.length; i++) {
    //     var item = cursorMethods[i];
    //     var method = item[0];
    //     var arg = item[1];
    //     cursor[method](arg);
    //   }
    //
    //   cursor.toArray(function(err, results) {
    //     results = results && results.map(utils.castToSnapshot);
    //     callback(err, results);
    //   });
    // });
  }

};

liveDBRethinkDB.prototype.query = function(livedb, cName, inputQuery, opts, callback) {
  // Regular queries are just a special case of queryProjected, but with fields=null (which livedb
  // will never pass naturally).
  this.queryProjected(livedb, cName, null, inputQuery, opts, callback);
};

liveDBRethinkDB.prototype.queryProjected = function(livedb, cName, fields, inputQuery, opts, callback) {
  var err; if (err = this._check(cName)) return callback(err);

  // To support livedb <=0.2.8
  if (typeof opts === 'function') {
    callback = opts;
    opts = {};
  }

  var query = utils.normalizeQuery(inputQuery);
  var err = this.checkQuery(query);
  if (err) return callback(err);

  // Use this.mongoPoll if its a polling query.
  if (opts.mode === 'poll' && this.mongoPoll) {
    var self = this;
    // This timeout is a dodgy hack to work around race conditions replicating the
    // data out to the polling target replica.
    setTimeout(function() {
      if (self.closed) return callback('db already closed');
      self._query(self.mongoPoll, cName, query, fields, callback);
    }, 300);
  } else {
    this._query(this.r, cName, query, fields, callback);
  }
};

liveDBRethinkDB.prototype.queryDocProjected = function(livedb, index, cName, docName, fields, inputQuery, callback) {
  var err;
  if (err = this._check(cName)) return callback(err);
  var query = utils.normalizeQuery(inputQuery);
  if (err = this.checkQuery(query)) return callback(err);

  // Run the query against a particular mongo document by adding an _id filter
  var queryId = query.$query._id;
  if (queryId) {
    delete query.$query._id;
    query.$query.$and = [{_id: docName}, {_id: queryId}];
  } else {
    query.$query._id = docName;
  }

  var projection = fields ? utils.projectionFromFields(fields) : false;

  function cb(err, doc) {
    callback(err, utils.castToSnapshot(doc));
  }

  if (this.mongoPoll) {
    var self = this;
    // Blah vomit - same dodgy hack as in queryProjected above.
    setTimeout(function() {
      if (self.closed) return callback('db already closed');
      self.mongoPoll.collection(cName).findOne(query, projection, cb);
    }, 300);
  } else {
    this.mongo.collection(cName).findOne(query, projection, cb);
  }
};

liveDBRethinkDB.prototype.queryDoc = function(livedb, index, cName, docName, inputQuery, callback) {
  this.queryDocProjected(livedb, index, cName, docName, null, inputQuery, callback);
};

// Test whether an operation will make the document its applied to match the
// specified query. This function doesn't really have enough information to know
// in all cases, but if we can determine whether a query matches based on just
// the operation, it saves doing extra DB calls.
//
// currentStatus is true or false depending on whether the query currently
// matches. return true or false if it knows, or null if the function doesn't
// have enough information to tell.
liveDBRethinkDB.prototype.willOpMakeDocMatchQuery = function(currentStatus, query, op) {
  return null;
};

// Does the query need to be rerun against the database with every edit?
liveDBRethinkDB.prototype.queryNeedsPollMode = function(index, query) {
  return query.hasOwnProperty('$orderby') ||
    query.hasOwnProperty('$limit') ||
    query.hasOwnProperty('$skip') ||
    query.hasOwnProperty('$count');
};


// Utility methods

// Return error string on error. Query should already be normalized with
// normalizeQuery below.
liveDBRethinkDB.prototype.checkQuery = function(query) {
  if (!this.allowJSQueries) {
    if (query.$query.$where != null)
      return "$where queries disabled";
    if (query.$mapReduce != null)
      return "$mapReduce queries disabled";
  }

  if (!this.allowAggregateQueries && query.$aggregate)
    return "$aggregate queries disabled";
};
