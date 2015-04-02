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
  if (query.constructor !== r.constructor) {
    throw new TypeError('Query must a ReQL query instance');
  }
  if (typeof mongoQueryObject !== 'object')  {
    throw new TypeError('Query object must bu an object');
  }
  // An object that will be directly passed to the filter method
  var filterQueryObject = {};
  // An object that will be transformed into a series of filter methods
  // that will be called individually
  var additionalQueryObjects = [];
  // Go Through every key/value in the object
  mongoQueryObject.forEach(function (value, key) {
    var firstKey = _.first(_.keys(value));
    // If the value is an object and has `$ne`, `$eq`, `$gte`, `$gt`, `$lt`, `$lte`
    if (typeof value === 'object' && firstKey.substring(0, 1) === '$') { // TODO: make more robust
      // Append it to a new object which will be added later
      additionalQueryObjects.push(parseMongoQuery(key, value));
    } else {
      filterQueryObject[key] = value;
    }
  });

  // Append filter parameters to our query object
  query = query.filter(filterQueryObject);
  // Append aditional parametesrs
  additionalQueryObjects.forEach(function (value) {
    query = query[value.type](value.query);
  });
};

module.exports = convertToReQLQuery;
