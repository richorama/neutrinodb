var request = require('request');
var qs = require('querystring');

function NeutrinoConnector(address){
   this.address = address;
}

NeutrinoConnector.prototype.get = function(table, key, cb){
	request.get(this.address + "/" + table + "/" + key, function(error, response, body){
		if (error) console.log(error);
		if (body == undefined){
			cb("Entity does not exist", undefined);
			return;
		}
		cb(error, JSON.parse(body));
	});
}

NeutrinoConnector.prototype.put = function(table, key, entity, cb){
	request.put(this.address + "/" + table + "/" + key, {json:entity}, function(error, response, body){
		if (error) console.log(error);
		if (cb) cb(error);
	});
}

NeutrinoConnector.prototype.del = function(table, key, cb){
	request.del(this.address + "/" + table + "/" + key, function(error, response, body){
		if (error) console.log(error);
		if (cb) cb(error);
	});
}

NeutrinoConnector.prototype.tables = function(cb){
	request.get(this.address + "/", function(error, response, body){
		if (error) console.log(error);
		cb(error, JSON.parse(body));
	});
}

NeutrinoConnector.prototype.createTable = function(table, cb){
	request.put(this.address + "/" + table, function(error, response, body){
		if (error) console.log(error);
		if (cb) cb(error);
	});
}

NeutrinoConnector.prototype.deleteTable = function(table, cb){
	request.del(this.address + "/" + table, function(error, response, body){
		if (error) console.log(error);
		if (cb) cb(error);
	});
}

NeutrinoConnector.prototype.query = function(table, query, options, cb){
	if (!cb){
		// options are optional!
		cb = options;
	}
	var params = {};
	if (query){
		params["$filter"] = query;
	}
	if (options && options.top){
		params["$top"] = options.top;
	}
	if (options && options.skip){
		params["$skip"] = options.skip;
	}

	var url = this.address + "/" + table + "?" + qs.stringify(params);

	request.get(url, function(error, response, body){
		if (error) console.log(error);
		cb(error, JSON.parse(body));
	});
}

NeutrinoConnector.prototype.registerView = function(name, table, query, cb){
	request.put(this.address + "/view/" + table + "/" + name + "?$filter=" + query, function(error, response, body){
		if (error) console.log(error);
		if (cb) cb();
	});
}

NeutrinoConnector.prototype.queryView = function(name, cb){
	request.get(this.address + "/view/" + name, function(error, response, body){
		if (error) console.log(error);
		cb(error, JSON.parse(body));
	});
}


var neutrino = new NeutrinoConnector("http://localhost:8080");



/* UNIT TESTS */

var assert = require('assert');

// test table creation
neutrino.createTable('newTable', function(error){
	neutrino.tables(function(error, data){
		assert(listContains(data, 'newTable'));
		neutrino.deleteTable('newTable', function(){
			neutrino.tables(function(error, data2){
				assert(!listContains(data2, 'newTable'));
				console.log("Test simple entity creation passed");
			});
		})
	});
});

// test put/get entity
neutrino.createTable('test', function(){
	neutrino.put('test', '1', {foo:"bar", baz:"qux"}, function(error){
		assert(!error);
		neutrino.get('test', '1', function(error, entity){
			assert(!error);
			assert(entity.foo == "bar");
			assert(entity.baz == "qux");
			assert(entity.key == "1");

			neutrino.del('test', '1', function(error){
				neutrino.get('test', '1', function(error2, entity2){
					assert(error2);
					assert(!entity2);
					neutrino.deleteTable('test');
					console.log("Test simple entity creation passed");
				});
			});

		});

		neutrino.get('test', 'NOT AN ENTITY', function(error, entity){
			assert(error);
			assert(!entity);
		});

		neutrino.get('NOT A TABLE', '1', function(error, entity){
			assert(error);
			assert(!entity);
		});

	});
});

neutrino.createTable('test2', function(){
	neutrino.put('test2', '1', {foo:"A"});
	neutrino.put('test2', '2', {foo:"B"});
	neutrino.put('test2', '3', {foo:"C"});
	neutrino.put('test2', '4', {foo:"A"}, function(){
		neutrino.query('test2', "foo eq 'A'", function(error, data){
			assert(data.length == 2);
			assert(data[0].key == '1');
			assert(data[1].key == '4');
		});

		neutrino.query('test2', "foo eq 'A'", { top: 1}, function(error, data){
			assert(data.length == 1);
			assert(data[0].key == '1');
			console.log("Test querying passed");
		});

	});
});


neutrino.createTable('test3', function(){
	neutrino.registerView('view1', 'test3', "active eq 'Y'", function(){
		neutrino.put('test3', '1', {active: "N"});
		neutrino.put('test3', '2', {active: "N"});
		neutrino.put('test3', 'Z', {active: "Y"});
		neutrino.put('test3', '4', {active: "N"});
		neutrino.put('test3', '5', {active: "N"}, function(){
			setTimeout(function(){
				neutrino.queryView('view1', function(error, body){
					assert(body.length == 1);
					assert(body[0].key == "Z");
					assert(body[0].active == 'Y');

					console.log("Test view registration passed");
				});
			},1000);
		});
	});
});


neutrino.createTable('test4', function(){
	neutrino.put('test4', '1', {active: "A"});
	neutrino.put('test4', '2', {active: "A"});
	neutrino.put('test4', '3', {active: "B"});
	neutrino.put('test4', '4', {active: "A"});
	neutrino.put('test4', '5', {active: "A"}, function(){
		neutrino.registerView('view2', 'test4', "active eq 'B'", function(){

			neutrino.queryView('view2', function(error, body){
				assert(body.length == 1);
				assert(body[0].key == "3");
				assert(body[0].active == 'B');

				console.log("Test view registration and index rebuild passed");
			});
		});
	});
});


function listContains(list, item){
	for (var i = 0; i < list.length; i++){
		if (list[i] === item) return true;
	}
	return false;
}
