var express = require('express');
var app = express();

app.use(express.bodyParser());

var odParser = require('odata-parser');
var assert = require('assert');

var db = require('./lib/db');


app.put("/view/:table/:view", function(req, res){
	var options = parseOptions(req);
	if (!options.filter){
		res.send(500);
		return;
	}
	db.registerView(req.params.view, req.params.table, options.filter, function(){
		res.send(200);
	});
});

app.get("/view/:view/:val?", function(req, res){
	db.queryView(req.params.view, req.params.val, function(error, data){
		res.send(data);
	});
});

app.get("/", function(req, res){
	db.readtables(function(data){
		res.send(data);
	});
});

app.put("/:table", function(req, res){
	db.puttable(req.params.table, function(result){
		if (result){
			res.send(200);
		} else {
			res.send(500);
		}
	});
});

app.delete("/:table", function(req, res){
	db.deltable(req.params.table, function(result){
		if (result){
			res.send(200);
		} else {
			res.send(500);
		}		
	});
});

app.get("/:table", function(req, res){
	var options = parseOptions(req);
	db.readRange(req.params.table, options, function(error, data){
		if (error) console.log(error);
		res.send(data);
	});
});

app.get("/:table/:key", function(req, res){
	db.get(req.params.table, req.params.key, function(error, data){
		res.send(data);
	});
});

app.put("/:table/:key", function(req, res){
	db.put(req.params.table, req.params.key, req.body, function(error, data){
		res.send(data);
	})
});

app.delete("/:table/:key", function(req, res){
	db.del(req.params.table, req.params.key, function(error){
		res.send(200);
	});
});

function parseOptions(req){
	var options = {skip:0};
	if (req.query["$skip"]){
		options.skip = parseInt(req.query["$skip"]);
	}
	if (req.query["$top"]){
		if (!req.query["$filter"]){
			// if there isn't a filter, we know in advance how many records to retrieve
			options.limit = parseInt(req.query["$top"]) + options.skip;
		}
		options.top = parseInt(req.query["$top"]);
	}

	if (req.query["$filter"]){
		var ast = odParser.parse("$filter=" + req.query["$filter"]);
		options.filter = ast["$filter"];
	}
	return options;
}

app.listen(8080);

/*
db.puttable('table1', function(){});
db.puttable('table2', function(){});
db.put('table1', 'key1', {foo:"bar"}, function(){});
db.put('table1', 'key2', {foo:"baz"}, function(){});
db.put('table1', 'key3', {foo:"qux"}, function(){});
*/