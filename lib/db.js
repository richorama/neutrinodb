var level = require('levelup');
var db = level('./mydb');
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
	console.log(table);
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

function evaluateAst(entity, filter){
	switch (filter.type){
		case "and":
			return evaluateAst(entity, filter.left) && evaluateAst(entity, filter.right);
		case "not":
			return !evaluateAst(entity, filter.left);
		case "or":
			return evaluateAst(entity, filter.left) || evaluateAst(entity, filter.right);

		case "eq":
			return evaluateAst(entity, filter.left) == evaluateAst(entity, filter.right);
		case "gt":
			return evaluateAst(entity, filter.left) > evaluateAst(entity, filter.right);
		case "ge":
			return evaluateAst(entity, filter.left) >= evaluateAst(entity, filter.right);
		case "lt":
			return evaluateAst(entity, filter.left) < evaluateAst(entity, filter.right);
		case "le":
			return evaluateAst(entity, filter.left) <= evaluateAst(entity, filter.right);
		case "ne":
			return evaluateAst(entity, filter.left) != evaluateAst(entity, filter.right);

		case "property":
			return entity[filter.name] ? entity[filter.name] : undefined;
		case "literal":
			return filter.value;
	}
}

function xor(a,b){return (a && !b) || (!a && b)};

function evaluateFilter(filter){
	switch (filter.type){
		case "and":
			// and can only tolerate Partition Keys on one side
			var leftResult = evaluateFilter(filter.left) 
			var rightResult = evaluateFilter(filter.right);
			if (leftResult.valid && rightResult.valid && xor(leftResult.PartitionKeys.length > 0, rightResult.PartitionKeys.length > 0)){
				return {PartitionKeys :leftResult.PartitionKeys.concat(rightResult.PartitionKeys), valid:false};
			} else {
				return {PartitionKeys : [], valid:false};
			}

		case "not":
			return {PartitionKeys : [], valid:false};
		case "or":
			// or can only tolerate partition keys on both sides
			var leftResult = evaluateFilter(filter.left) 
			var rightResult = evaluateFilter(filter.right);
			if (leftResult.valid && rightResult.valid && leftResult.PartitionKeys.length > 0 && rightResult.PartitionKeys.length > 0){
				return {PartitionKeys: leftResult.PartitionKeys.concat(rightResult.PartitionKeys), valid:true};
			} else {
				return {PartitionKeys : [], valid:false};
			}

		case "eq":
			if (filter.left.type == 'property' && filter.left.name == 'PartitionKey'){
				return {PartitionKeys : [filter.right.value], valid:true};
			} else if (filter.right.type == 'property' && filter.right.name == 'PartitionKey'){
				return {PartitionKeys : [filter.left.value], valid:true};
			} else {
				return {PartitionKeys : [], valid:true};
			}
		case "gt":
		case "ge":
	
		case "lt":
		case "le":
		
		case "ne":
			return {PartitionKeys : [], valid:true};

		/*
		case "property":
			return entity[filter.name] ? entity[filter.name].value : undefined;
		case "literal":
			return filter.value;
		*/

	}
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