var metaOperators = {
  $comment: true,
  $explain: true,
  $hint: true,
  $maxScan: true,
  $max: true,
  $min: true,
  $orderby: true,
  $returnKey: true,
  $showDiskLoc: true,
  $snapshot: true,
  $count: true,
  $aggregate: true
};

var cursorOperators = {
  $limit: 'limit',
  $skip: 'skip'
};

function isValidCName(cName) {
  return !(/_ops$/.test(cName)) && cName !== 'system';
}

function extractCursorMethods(query) {
  var out = [];
  for (var key in query) {
    if (cursorOperators[key]) {
      out.push([cursorOperators[key], query[key]]);
      delete query[key];
    }
  }
  return out;
}

function normalizeQuery(inputQuery) {
  // Box queries inside of a $query and clone so that we know where to look
  // for selctors and can modify them without affecting the original object
  var query;
  if (inputQuery.$query) {
    query = shallowClone(inputQuery);
    query.$query = shallowClone(query.$query);
  } else {
    query = {$query: {}};
    for (var key in inputQuery) {
      if (metaOperators[key] || cursorOperators[key]) {
        query[key] = inputQuery[key];
      } else {
        query.$query[key] = inputQuery[key];
      }
    }
  }

  // Deleted documents are kept around so that we can start their version from
  // the last version if they get recreated. When they are deleted, their type
  // is set to null, so don't return any documents with a null type.
  if (!query.$query._type) query.$query._type = {$ne: null};

  return query;
}

function castToDoc(docName, data) {
  var doc = (
    typeof data.data === 'object' &&
    data.data !== null &&
    !Array.isArray(data.data)
  ) ?
    shallowClone(data.data) :
    {_data: (data.data === void 0) ? null : data.data};
  doc._type = data.type || null;
  doc._v = data.v;
  doc._m = data.m;
  doc.id = docName;
  return doc;
}

function castToSnapshot(doc) {
  if (!doc) return;
  var type = doc._type;
  var v = doc._v;
  var docName = doc.id;
  var data = doc._data;
  var meta = doc._m;
  if (data === void 0) {
    doc = shallowClone(doc);
    delete doc._type;
    delete doc._v;
    delete doc.id;
    delete doc._m;
    return {
      data: doc,
      type: type,
      v: v,
      docName: docName,
      m: meta,
    };
  }
  return {
    data: data,
    type: type,
    v: v,
    docName: docName,
    m: meta
  };
}

function shallowClone(object) {
  var out = {};
  for (var key in object) {
    out[key] = object[key];
  }
  return out;
}

/**
 * In MongoDB, projections are a way to only select a subset of fields in a document
 * This is pretty similar to a `pluck` operation in ReQL
 */
function projectionFromFields(fields) {
  var pluckedFields = fields.slice();
  pluckedFieldsields.push('_v');
  pluckedFieldsields.push('_m');
  pluckedFieldsields.push('_type');
  return pluckedFields;
}

exports.isValidCName = isValidCName;
exports.extractCursorMethods = extractCursorMethods;
exports.normalizeQuery = normalizeQuery;
exports.castToDoc = castToDoc;
exports.castToSnapshot = castToSnapshot;
exports.shallowClone = shallowClone;
exports.projectionFromFields = projectionFromFields;
