# Mocha test using livedb's snapshot tests
rethinkdbdash = require 'rethinkdbdash'
liveDBRethinkDB = require './rethinkdb'
assert = require 'assert'

# Clear mongo
clear = (callback) ->
  r = rethinkdbdash({ host: 'localhost', port: '28015', db: 'sharejs' })
  r
    .tableDrop 'testcollection'
    .run()
    .catch(() ->)
    .then () ->
      r
        .tableDrop 'testcollection_ops'
        .run()
        .catch(() ->)
        .then () ->
          console.log('Done Clearing')
          callback()

create = (callback) ->
  # clear () ->
  #   callback liveDBRethinkDB { host: 'localhost', port: '28015', db: 'sharejs' }
  callback liveDBRethinkDB({ host: 'localhost', port: '28015', db: 'sharejs' })

describe 'rethinkdb', ->
  # afterEach () ->
  #   console.log('After Each');
  #   r = rethinkdbdash({ host: 'localhost', port: '28015', db: 'sharejs' })
  #   r
  #     .tableList()
  #     .run()
  #     .then (tableList) ->
  #       console.log('tableList');
  #       console.log(tableList);


  describe 'raw', ->
    beforeEach (done) ->
      @r = rethinkdbdash { host: 'localhost', port: '28015', db: 'sharejs' }
      create (@db) => done()

    it 'adds an index for ops', (done) -> create (db) =>
      db.writeOp 'testcollection', 'foo', {v:0, create:{type:'json0'}}, (err) =>
        # The problem here is that the index might not have been created yet if
        # the database is busy, which makes this test flakey. I'll put a
        # setTimeout for now, but if there's more problems, it might have to be
        # rewritten.
        @r.table('testcollection_ops').indexList().run().then (indexes) ->
          assert.deepEqual(indexes, ['name', 'v']);
          # We should find an index with [[ 'name', 1 ], [ 'v', 1 ]]
          # for name, idx of indexes
          #  # if JSON.stringify(idx) is '[["name",1],["v",1]]'
          done()

          # throw Error "Could not find index in ops db - #{JSON.stringify(indexes)}"

    it 'does not allow editing the system collection', (done) ->
      @db.writeSnapshot 'system', 'test', {type:'json0', v:5, m:{}, data:{x:5}}, (err) =>
        assert.ok err
        @db.getSnapshot 'system', 'test', (err, data) ->
          assert.ok err
          assert.equal data, null
          done()

    it 'defaults to the version of the document if there are no ops', (done) ->
      @db.writeSnapshot 'testcollection', 'versiontest', {type: 'json0', v: 3, data:{x:5}}, (err) =>
        @db.getVersion 'testcollection', 'versiontest', (err, v) =>
          throw Error err if err
          assert.equal v, 3
          done()


    describe 'query', ->
      it 'returns data in the collection', (done) ->
        snapshot = {type:'json0', v:5, m:{}, data:{x:5, y:6}}
        @db.writeSnapshot 'testcollection', 'test', snapshot, (err) =>
          @db.query 'unused', 'testcollection', {x:5}, {}, (err, results) ->
            throw Error err if err
            'no error'.log();
            delete results[0].docName
            results.log();
            [snapshot].log();
            assert.deepEqual results, [snapshot]
            done()

      it 'returns nothing when there is no data', (done) ->
        @db.query 'unused', 'testcollection', {x:5}, {}, (err, results) ->
          throw Error err if err
          assert.deepEqual results, []
          done()

      it 'does not allow $where queries', (done) ->
        @db.query 'unused', 'testcollection', {$where:"true"}, {}, (err, results) ->
          assert.ok err
          assert.equal results, null
          done()

      it '$distinct should perform distinct operation', (done) ->
        snapshots = [
          {type:'json0', v:5, m:{}, data:{x:1, y:1}},
          {type:'json0', v:5, m:{}, data:{x:2, y:2}},
          {type:'json0', v:5, m:{}, data:{x:3, y:2}}
        ]
        @db.writeSnapshot 'testcollection', 'test1', snapshots[0], (err) =>
          @db.writeSnapshot 'testcollection', 'test2', snapshots[1], (err) =>
            @db.writeSnapshot 'testcollection', 'test3', snapshots[2], (err) =>
              @db.query 'unused', 'testcollection', {$distinct: true, $field: 'y', $query: {}}, {}, (err, results) ->
                throw Error err if err
                assert.deepEqual results.extra, [1,2]
                done()

      it '$aggregate should perform aggregate command', (done) ->
        snapshots = [
          {type:'json0', v:5, m:{}, data:{x:1, y:1}},
          {type:'json0', v:5, m:{}, data:{x:2, y:2}},
          {type:'json0', v:5, m:{}, data:{x:3, y:2}}
        ]
        @db.allowAggregateQueries = true

        @db.writeSnapshot 'testcollection', 'test1', snapshots[0], (err) =>
          @db.writeSnapshot 'testcollection', 'test2', snapshots[1], (err) =>
            @db.writeSnapshot 'testcollection', 'test3', snapshots[2], (err) =>
              @db.query 'unused', 'testcollection', {$aggregate: [{$group: {_id: '$y', count: {$sum: 1}}}, {$sort: {count: 1}}]}, {}, (err, results) ->
                throw Error err if err
                assert.deepEqual results.extra, [{_id: 1, count: 1}, {_id: 2, count: 2}]
                done()

      it 'does not let you run $aggregate queries without options.allowAggregateQueries', (done) ->
        @db.query 'unused', 'testcollection', {$aggregate: [{$group: {_id: '$y', count: {$sum: 1}}}, {$sort: {count: 1}}]}, {}, (err, results) ->
          assert.ok err
          done()

      it 'does not allow $mapReduce queries by default', (done) ->
        snapshots = [
          {type:'json0', v:5, m:{}, data:{player: 'a', round: 1, score: 5}},
          {type:'json0', v:5, m:{}, data:{player: 'a', round: 2, score: 7}},
          {type:'json0', v:5, m:{}, data:{player: 'b', round: 1, score: 15}}
        ]
        @db.writeSnapshot 'testcollection', 'test1', snapshots[0], (err) =>
          @db.writeSnapshot 'testcollection', 'test2', snapshots[1], (err) =>
            @db.writeSnapshot 'testcollection', 'test3', snapshots[2], (err) =>
              @db.query 'unused', 'testcollection',
                $mapReduce: true,
                $map: ->
                  emit @.player, @score
                $reduce: (key, values) ->
                  values.reduce (t, s) -> t + s
                $query: {}
              , {}, (err, results) ->
                assert.ok err
                assert.equal results, null
                done()

      it '$mapReduce queries should work when allowJavaScriptQuery == true', (done) ->
        snapshots = [
          {type:'json0', v:5, m:{}, data:{player: 'a', round: 1, score: 5}},
          {type:'json0', v:5, m:{}, data:{player: 'a', round: 2, score: 7}},
          {type:'json0', v:5, m:{}, data:{player: 'b', round: 1, score: 15}}
        ]
        @db.allowJSQueries = true

        @db.writeSnapshot 'testcollection', 'test1', snapshots[0], (err) =>
          @db.writeSnapshot 'testcollection', 'test2', snapshots[1], (err) =>
            @db.writeSnapshot 'testcollection', 'test3', snapshots[2], (err) =>
              @db.query 'unused', 'testcollection',
                $mapReduce: true,
                $map: ->
                  emit @.player, @score
                $reduce: (key, values) ->
                  values.reduce (t, s) -> t + s
                $query: {}
              , {}, (err, results) ->
                throw Error err if err
                assert.deepEqual results.extra, [{_id: 'a', value: 12}, {_id: 'b', value: 15}]
                done()

    describe 'queryProjected', ->
      it 'returns only projected fields', (done) ->
        @db.writeSnapshot 'testcollection', 'test', {type:'json0', v:5, m:{}, data:{x:5, y:6}}, (err) =>
          @db.queryProjected 'unused', 'testcollection', {y:true}, {x:5}, {}, (err, results) ->
            throw Error err if err
            assert.deepEqual results, [{type:'json0', v:5, m:{}, data:{y:6}, docName:'test'}]
            done()

      it 'returns no data for matching documents if fields is empty', (done) ->
        @db.writeSnapshot 'testcollection', 'test', {type:'json0', v:5, m:{}, data:{x:5, y:6}}, (err) =>
          @db.queryProjected 'unused', 'testcollection', {}, {x:5}, {}, (err, results) ->
            throw Error err if err
            assert.deepEqual results, [{type:'json0', v:5, m:{}, data:{}, docName:'test'}]
            done()

    describe 'queryDoc', ->
      it 'returns null when the document does not exist', (done) ->
        @db.queryDoc 'unused', 'unused', 'testcollection', 'doesnotexist', {}, (err, result) ->
          throw Error err if err
          assert.equal result, null
          done()

      it 'returns the doc when the document does exist', (done) ->
        snapshot = {type:'json0', v:5, m:{}, data:{x:5, y:6}}
        @db.writeSnapshot 'testcollection', 'test', snapshot, (err) =>
          @db.queryDoc 'unused', 'unused', 'testcollection', 'test', {}, (err, result) ->
            throw Error err if err
            snapshot.docName = 'test'
            assert.deepEqual result, snapshot
            done()

      it 'does not allow $where queries', (done) ->
        @db.queryDoc 'unused', 'unused', 'testcollection', 'somedoc', {$where:"true"}, (err, result) ->
          assert.ok err
          assert.equal result, null
          done()

    describe 'queryDocProjected', ->
      beforeEach (done) ->
        @snapshot = {type:'json0', v:5, m:{}, data:{x:5, y:6}}
        @db.writeSnapshot 'testcollection', 'test', @snapshot, (err) =>
          @snapshot.docName = 'test'
          throw Error err if err
          done()

      it 'returns null when the document does not exist', (done) ->
        @db.queryDocProjected 'unused', 'unused', 'testcollection', 'doesnotexist', {x:true}, {}, (err, result) ->
          throw Error err if err
          assert.equal result, null
          done()

      it 'returns the requested fields of the doc', (done) ->
        @db.queryDocProjected 'unused', 'unused', 'testcollection', 'test', {x:true}, {}, (err, result) =>
          throw Error err if err
          @snapshot.data = {x:5}
          assert.deepEqual result, @snapshot
          done()

      it 'returns empty data if no fields are requested', (done) ->
        @db.queryDocProjected 'unused', 'unused', 'testcollection', 'test', {}, {}, (err, result) =>
          throw Error err if err
          @snapshot.data = {}
          assert.deepEqual result, @snapshot
          done()


  # require('livedb/test/snapshotdb') create
  # require('livedb/test/oplog') create
