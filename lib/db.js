var level = require('levelup');
var subLevel = require('level-sublevel');
var mappedIndex = require('level-mapped-index');

var db = level('./mydb');
db = subLevel(db)
db = mappedIndex(db)


var async = require('async');
var assert = require('assert');

module.exports.get = get;
module.exports.put = put;
module.exports.del = del;

module.exports.gettable = gettable;
module.exports.puttable = puttable;
module.exports.deltable = deltable;
module.exports.readtables = readtables;

module.exports.readRange = readRange;

module.exports.registerView = registerView;
module.exports.queryView = queryView;

function levelKey(table, key){
	return table + "~" + key + "~";
}


// retrieve a value from the database by key
function get(table, key, cb){
	gettable(table, function(exists){
		if (!exists){
			cb("Table does not exist", undefined);
			// should we throw an error here?
			return;
		}
		db.get(levelKey(table,key), function(error, item){
			if (item){
				cb(error, JSON.parse(item));
			} else {
				cb(error, undefined);
			}
		});	
	});
}

// save a value to the database
function put(table, key, value, cb){
	gettable(table, function(exists){
		if (!exists){
			cb("Table does not exist");
			// should we throw an error here?
			return;
		}
		if (value == null){
			cb("Entity is null");
			// should we throw an error here?
			return;	
		}

		value.key = key;

		db.put(levelKey(table,key), JSON.stringify(value), function(error){
			cb(error);
		});		
	});
}

function del(table, key, cb){
	gettable(table, function(exists){
		if (!exists){
			cb("Table does not exist");
			// should we throw an error here?
			return;
		}

		db.del(levelKey(table,key), function(){
			if (cb){
				cb();
			}
		});
	});
}

function gettable(tableName, cb){
	db.get("~__tables~" + tableName, function(error, data){
		cb(data);
	});
}

function puttable(tableName, cb){
	db.put("~__tables~" + tableName, tableName, function(){
		cb(true);
	});
}

function deltable(tableName, cb){
	db.del("~__tables~" + tableName, function(){
		if (cb) {
			cb(true);
		}
	});
	deleteRange({start:tableName + "~", end:tableName + "~~"}, function(){

	});
}

function readtables(cb){
	var items = [];
	var readStream = db.createReadStream({keys:false, start:"~__tables~", end: "~__tables~~"});

	readStream.on('data', function (data) {
		items.push(data);
	});

	readStream.on('end', function () {
		cb(items);
	});
	readStream.on('error', function (error) {
		console.log(error)
	});
}

function clone(a) {
	return JSON.parse(JSON.stringify(a));
}

function removeDupes(x){
	var tmp = {};
	var output = [];
	x.forEach(function(key){
		tmp[key] = key;
	});
	for (var key in tmp){
		output.push(key);
	}
	return output;
}


var testDupes = removeDupes([1,2,1,3]);
assert(testDupes.length === 3);


function readRange(table, options, cb){
	gettable(table, function(exists){
		if (!exists){
			cb("Table does not exist");
			// should we throw an error here?
			return;
		}

		// execute as before	
		options.start = table + "~";
		options.end = table + "~~";
		executeRange(table, options, cb);
	});
}

function executeRange(table, options, cb){

	var items = [];
	options.keys = false;
	var index = 0;
	var readStream = db.createReadStream(options);
	readStream.on('data', function (data) {
		
		if (!options.filter || evaluateAst(JSON.parse(data), options.filter)){
			if (index >= options.skip && (!options.top || items.length < options.top)){
				
				items.push(JSON.parse(data));

				if (options.top && items.length >= options.top){
					// optimization to stop the read stream once we have reached the require entity count
					readStream.destroy();
				}
			}
			index++;
		}
	});
	readStream.on('end', function () {
		cb(undefined, items);
	});
	readStream.on('error', function (error) {
    	console.log(error)
  	});
}

function deleteRange(options, cb){

	var readStream = db.createKeyStream(options);
	var count = 0;
	readStream.on('data', function (data) {
		db.del(data);
		count++;
	});
	readStream.on('end', function () {
		if (cb){
			cb(undefined, count);
		}
	});
	readStream.on('error', function (error) {
    	console.log(error)
  	});	
}

function evaluateAst(entity, filter, param){
	switch (filter.type){
		case "and":
			return evaluateAst(entity, filter.left, param) && evaluateAst(entity, filter.right, param);
		case "not":
			return !evaluateAst(entity, filter.left, param);
		case "or":
			return evaluateAst(entity, filter.left, param) || evaluateAst(entity, filter.right, param);

		case "eq":
			return evaluateAst(entity, filter.left, param) == evaluateAst(entity, filter.right, param);
		case "gt":
			return evaluateAst(entity, filter.left, param) > evaluateAst(entity, filter.right, param);
		case "ge":
			return evaluateAst(entity, filter.left, param) >= evaluateAst(entity, filter.right, param);
		case "lt":
			return evaluateAst(entity, filter.left, param) < evaluateAst(entity, filter.right, param);
		case "le":
			return evaluateAst(entity, filter.left, param) <= evaluateAst(entity, filter.right, param);
		case "ne":
			return evaluateAst(entity, filter.left, param) != evaluateAst(entity, filter.right, param);

		case "property":
			return entity[filter.name] ? entity[filter.name] : undefined;
		case "literal":
			if (filter.value == "?"){
				return param;
			} else {
				return filter.value;
			}
	}
}

function countParamsAst(count, filter){
	switch (filter.type){
		case "and":
		case "not":
		case "or":
		case "eq":
		case "gt":
		case "ge":
		case "lt":
		case "le":
		case "ne":
			return countParamsAst(count, filter.left) + countParamsAst(count, filter.right);
		case "property":
			return 0;
		case "literal":
			return (filter.value == "?")  ? 1 : 0;
	}
}


function extractIndexFieldFromAst(properties, filter){
	switch (filter.type){
		case "eq":
			if (filter.left.type == "property" && filter.right.type == "literal" && filter.right.value == "?") {
				properties.push(filter.left.name);
				return properties;
			} else if (filter.right.type == "property" && filter.left.type == "literal" && filter.left.value == "?"){
				properties.push(filter.right.name);
				return properties;
			} else 
				return properties;
		case "and":
		case "not":
		case "or":
		case "gt":
		case "ge":
		case "lt":
		case "le":
		case "ne":
			return extractIndexFieldFromAst(properties, filter.left).concat(extractIndexFieldFromAst(properties, filter.right));
		case "property":
			return properties;
		case "literal":
			return properties;
	}
}


function registerView(name, table, ast, cb){

	var field = undefined;
	if (ast) {
		var count = countParamsAst(0, ast);
		if (count > 1) cb && cb("cannot have a query with more than one parameter");
		var fields = extractIndexFieldFromAst([], ast);
		if (fields.length == 1){
			field = fields[0];
		}
	}

	db.registerIndex(name, function (key, value, emit) {

		//console.log("view " + name);
		//console.log(field);

		if (key[0] == "~") return; // if this is a special key, ignore it
		if (key.split('~')[0] != table) return; // wrong table

		value = JSON.parse(value)
		
		if (field){
			if (evaluateAst(value, ast, value[field])){
				emit(value[field]);
			}
		} else {
			if (evaluateAst(value, ast)){
				emit("Y");
			}			
		}

	});

	// flush all the keys to build the index
	var readStream = db.createReadStream({start:table + "~", end:table + "~~"});
	readStream.on('data', function (data) {
		db.put(data.key, data.value);
	});
	readStream.on('end', function () {
		cb && cb();
	});
	readStream.on('error', function (error) {
		cb && cb(error);
		cb = null;
  	});	
}

function queryView(name, param, cb){
	if (undefined == param){
		param = "Y"
	}
	console.log(param);
	db.getBy(name, param, function(error, data){
		var data2 = [];
		if (!data) {
			data = [];
		}
		data.forEach(function(x){
			data2.push(JSON.parse(x.value));
		});
		cb(error, data2);
	});

}


/*
puttable("foobar", function(){
	gettable("foobar", function(result){
		if (!result){
			throw new Error("foobar table not created");
			return;
		}
		readtables(function(tables){
			var found = false;
			tables.forEach(function(x){
				if (x == "foobar"){
					found = true;
				}
			});
			if (!found){
				throw new Error("foobar not found in list");
				return;
			}
			deltable("foobar", function(){
				gettable("foobar", function(result2){
					if (result2){
						throw new Error("foobar not deleted");
						return;
					}
					console.log("test passed");
				});

			});
		});
	});
})
*/