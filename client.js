var request = require('request');

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


function listContains(list, item){
	for (var i = 0; i < list.length; i++){
		if (list[i] === item) return true;
	}
	return false;
}