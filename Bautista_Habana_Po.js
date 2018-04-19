//Lab Activity 2

//1
map1 = function (){
	emit(
		this.course
	,
		1
	);
}

reduce1 = function(key,values) {
	var total = 0;
	for(var i = 0; i < values.length; i++) {
		total += values[i];
	}
	return total;
}

results1 = db.runCommand({
	mapReduce: 'classmates',
	map: map1,
	reduce: reduce1,
	out: 'classmates.answer1'
});

db.classmates.answer1.find().pretty()

//2

map2 = function() {
	emit(this.year, 1);
}

reduce2 = function(key,values) {
	var total = 0;
	for(var i = 0; i < values.length; i++) {
		total += values[i];
	}
	return total;
}

results2 = db.runCommand({
	mapReduce: 'classmates',
	map: map2,
	reduce: reduce2,
	out: 'classmates.answer2'
});

db.classmates.answer2.find().pretty()

//3

map3 = function() {
	emit({
		year: this.year,
		course: this.course
	}, 1);
}

reduce3 = function(key,values) {
	var total = 0;
	for(var i = 0; i < values.length; i++) {
		total += values[i];
	}
	return total;
}

results3 = db.runCommand({
	mapReduce: 'classmates',
	map: map3,
	reduce: reduce3,
	out: 'classmates.answer3'
});

db.classmates.answer3.find().pretty()

//4

map4 = function() {
	emit({
		initial: this.last_name.charAt(0) + this.first_name.charAt(0)
	}, {
		count: 1,
		full_names: this.last_name + ', ' + this.first_name + ''
	});
}

reduce4 = function(key,values) {
	var total = 0;
	var names = [];
	for(var i = 0; i < values.length; i++) {
		total += values[i].count;
		names[i] = values[i].full_names;
	}
	return {count: total, full_names: names};
}

results4 = db.runCommand({
	mapReduce: 'classmates',
	map: map4,
	reduce: reduce4,
	out: 'classmates.answer4'
});

db.classmates.answer4.find().pretty();
