NeutrinoDB
==========

A json document database built on node.js and leveldb

**This database is experimental, not for production use**

Motivation
----------

This is a learning exercise. I wanted to combine [LevelDB](https://code.google.com/p/leveldb/) (using the [levelup](https://npmjs.org/package/levelup) module)  
with [express](https://npmjs.org/package/express) to provide an HTTP interface and [odata-parser](https://npmjs.org/package/odata-parser) to provide a querying syntax.


Architecture
------------

Neutrino is a web server, you read/write the data over http using either raw http requests, or the client library.

Install and start the Server
----------------------------

```
$ git clone https://github.com/richorama/neutrinodb.git
$ cd neutrinodb/database
$ [sudo] npm install
$ node server
```

Connect using the client
------------------------

Install the neutrino package.

```
$ npm install neutrinodb
```

In your node application you can create a database client like this:

```
// create a neutrino client, by specifying the location of the server 
// (no trailing slash)
var client = require('neutrino')('http://localhost:8080');
```

Create, list and delete tables
------------------------------

Neutrino has the concept of tables, you can create, list and delete them like this.

```
// create a new table called 'table1'
client.createTable('table1', function(err){
	if (err) console.log(err);
})

// list all tables in the database
client.tables(function(error, tables){
	if (err) console.log(err);
	tables.forEach(function(table){
		console.log(table);
	});
});

// delete 'table2' from the database. This will delete all data in the table
client.deleteTable('table2', function(err){
	if (err) console.log(err);
})

```

Create, get, list and delete objects
-------------------------------

You can add, remove and list objects in your table like this:

```
// create an object withe the 'A' key 
client.put('test1', 'A', {foo:"bar"}, function(err){
	if (err) console.log(err);
});
	
// get object 'A' back
client.get('test1', 'A', function(err, data){
	if (err) console.log(err);
	console.log(data); 
});	

// delete object 'A'
client.del('test1', 'A', function(err){
	if (err) console.log(err);
});

// retrieve all records from table 1
client.query('table1', function(err, objects){
	objects.forEach(function(object){
		console.log(object);
	});
});


```

Querying tables
---------------

Tables can be queried using the OData syntax:

```
// all objects where foo = 'bar'.
client.query('table1', "foo eq 'bar'", function(err, data){ ... });

// first 5 objects matching the query
client.query('table1', "foo eq 'bar'", {top: 5}, function(err, data){ ... });

// second page of 5 objects matching the query
client.query('table1', "foo eq 'bar'", {top: 5, skip: 5}, function(err, data){ ... });

```

Note that querying a table like this will result in a full table scan.

If you only want one (or a few) records, use the 'top' option to improve performance.

Views
-----

To avoid full table scans, you can register 'views' which will be indexed for you by the database.

```
client.registerView('view1', 'table1', "foo eq 'bar'", function(err){ ... });

client.queryView('view1', function(err, data){ ... });
```

Views can also be parameterised, on the condition that only one parameter is used, and that's used to test if a property is 'eq'.

```
client.registerView('view1', 'table1', "foo eq '?'", function(err){ ... });

client.queryView('view1', 'bar', function(err, data){ ... });
client.queryView('view1', 'baz', function(err, data){ ... });

```

At the moment views cannot be deleted.


License
-------

MIT