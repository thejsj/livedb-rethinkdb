var r = require('rethinkdbdash')();
var _ = require('lodash');

/**
 * Takes a key/value pair and converts it into an object that can
 * later be easily be appended into a ReQL query
 *
 * { 'type': filter, 'value': r.row(property).gt(value) }
 *
 * @param key <String>
 * @param value <Object>
 * @param queryObject <Object>
 */
var parseMongoQuery = function (key, value) {
  var queryTypes = {
    '$ne' : 'ne',
    '$eq' : 'eq',
    '$gt' : 'gt',
    '$gte': 'ge',
    '$lt' : 'lt',
    '$lte': 'le'
  };
  var firstKey = _.first(_.keys(value));

  if (_.keys(queryTypes).indexOf(firstKey) === -1) {
    // If we can't find the value in the keys, we'll just return it
    var obj  = {};
    obj[key] = value;
    return {
      type: 'filter',
      query: obj
    };
  }
  return {
    type: 'filter',
    query: r.row(key)[queryTypes[firstKey]](value[firstKey])
  };
};

/**
 * Takes a ReQL query object (r.table("table_name")) and a mongo query object
 * and appends the queries in the mongo query object to the ReQL query. This
 * mostly convert mongo style filters (`$ne`, `$gt`, ...) to ReQL `filter`s.
 *
 * Returns a modified ReQL query.
 *
 * @param query <Object>
 * @param mongoQueryObject <Object>
 * @return query <Object>
 */
var convertToReQLQuery = function (query, mongoQueryObject) {
  if (query.constructor !== r._Term) {
    throw new TypeError('Query must be a ReQL query instance');
  }
  if (typeof mongoQueryObject !== 'object')  {
    throw new TypeError('Query object must bu an object');
  }

  // An object that will be directly passed to the filter method
  var filterQueryObject = {};
  // An object that will be transformed into a series of filter methods
  // that will be called individually
  var additionalQueryObjects = [];


  /**
   * $query
   * Go Through every key/value in the object
   */
  _.each(mongoQueryObject.$query, function (value, key) {
    var firstKey = _.first(_.keys(value));
    // If the value is an object and has `$ne`, `$eq`, `$gte`, `$gt`, `$lt`, `$lte`
    if (typeof value === 'object' && firstKey.substring(0, 1) === '$') { // TODO: make more robust
      // Append it to a new object which will be added later
      additionalQueryObjects.push(parseMongoQuery(key, value));
    } else {
      // Map mongo's _id to id
      if (key === '_id') key = 'id';
      filterQueryObject[key] = value;
    }
  });

  /**
   * $distinct
   */
  if (mongoQueryObject.$distinct !== undefined && mongoQueryObject.$field !== undefined) {
    additionalQueryObjects.push({
      type: 'getField',
      query: mongoQueryObject.$field
    });
    additionalQueryObjects.push({
      type: 'distinct', query: undefined
    });
  }

  /**
   * $aggregate
   */
  if (mongoQueryObject.$aggregate !== undefined) {

    // Combine all parts of the query
    var $agg = _.reduce(mongoQueryObject.$aggregate, function (obj, n) {
      _.each(n, function (value, key) {
        obj[key] = value;
      });
      return obj;
    }, {});

    // This is what a ReQL query looks like
    // .group(r.row('data')('y')).ungroup()('reduction')
    // .map(function (row) {
    //   return {
    //     _id: row.nth(0)('data')('y'),
    //     count: row.count()
    //   }
    // });

    // Add into the query objects
    if ($agg.$group) {
      var searchKey = $agg.$group._id.substring(1);
      additionalQueryObjects.push({
        type: 'group',
        // Remove the first char, since it's a dollar sign $
        query: searchKey
      });
      additionalQueryObjects.push({
        type: 'ungroup', query: undefined,
      });
      additionalQueryObjects.push({
        type: 'map',
        query: function (row) {
          return { _id: row.nth(0)(searchKey), count: row.count()  };
        }
      });
    }
  }

  // Append filter parameters to our query object
  query = query.filter(filterQueryObject);
  // Append aditional parametesrs
  additionalQueryObjects.forEach(function (value) {
    if (typeof query[value.type] === 'function') {
      if (typeof value.query !== undefined) {
        query = query[value.type](value.query);
      } else {
        query = query[value.type]();
      }
    } else {
      throw new TypeError(value.type + ' is not a ReQL method');
    }
  });
  return query;
};

module.exports = convertToReQLQuery;
