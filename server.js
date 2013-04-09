var express = require('express');
var app = express();

var odParser = require('odata-parser');
var assert = require('assert');

var db = require('./lib/db');


app.get("/", function(req, res){
	db.readtables(function(data){
		res.send(data);
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

/*
	var tableName = req.params[0];
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

	db.readRange(tableName, options, function(error, items){
		if (error){
			var errorText = atomWriter.formatError("Error", error);
			res.setHeader("Content-Length", errorText.length);
			res.send(404, errorText);			
			return;
		}
		var content = atomWriter.toAtomFeed(req.params[0], items);
		res.setHeader("Content-Type", "application/atom+xml;charset=utf-8");
		res.setHeader("Content-Length", content.length);
		res.send(200, content);
	});

*/



app.listen(8080);


db.puttable('table1', function(){});
db.puttable('table2', function(){});
db.put('table1', 'key1', {foo:"bar"}, function(){});
db.put('table1', 'key2', {foo:"baz"}, function(){});
db.put('table1', 'key3', {foo:"qux"}, function(){});