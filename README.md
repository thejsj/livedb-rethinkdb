# livedb-rethinkdb

RethinkDB database adapter for [livedb](https://github.com/share/livedb).

The module’s api is identical to [livedb-mongo](https://github.com/share/livedb-mongo) and works in the same way.

snapshots are stored where you'd expect (the named table with
\_id=docname). operations are stored in `collection_ops`. if you have a
users collection, the operations are stored in `users_ops`. if you have a
document called `fred`, operations will be stored in documents called `fred
v0`, `fred v1`, `fred v2` and so on.

json document snapshots in livedb-rethinkdb are unwrapped so you can use reql
queries directly against json documents. (they just have some extra fields in
    the form of `_v` and `_type`). you should always use livedb to edit
documents - don't just edit them directly in rethinkdb. you'll get weird behaviour
if you do.

## Install

```
npm install livedb-rethinkdb
```

## Usage

livedb-rethhinkdb wraps [rethinkdbdash](). it
passes all the arguments straight to rethinkdbdash's constructor. `npm install
livedb-rethinkdb` then create your database wrapper using the same arguments you
would pass to rethinkdbdash:

```javascript
var livedbrethinkdb = require('livedb-rethinkdb');
// Pass your RethinkDB parameters
var r = livedbrethinkdb({
  host: 'localhost',
  port: 28015,
  db: 'sharejs'
});

var livedb = require('livedb').client(livedbrethinkdb);
```

if you prefer, you can instead create a rethinkdbdash instance yourself and pass it to livedb-rethinkdb:
```javascript
var rethinkdbdash = require('rethinkdbdash');
var r = rethinkdbdash({
  host: 'localhost',
  port: 28015,
  db: 'sharejs'
});

var livedbrethinkdb = require('livedb-rethinkdb');
var rethinkdb = livedbrethinkdb(r);
```
## Test

Run the tests using: 
```
npm test
```
## Credit 

This project is a fork of [livedb-mongo](https://github.com/share/livedb-mongo).

## MIT License
Copyright (c) 2015 by RethinkDB.

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.
