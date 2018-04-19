var config = {
  "_id": "finals2",
  "version" : 1,
  "members" :   [
    {
      "_id" : 0,
      "host" : "localhost:27017",
      "priority" : 4
    },
    {
      "_id" : 1,
      "host" : "localhost:27018",
      "priority" : 3
    },
    {
      "_id" : 2,
      "host" : "localhost:27019",
      "priority" : 2
    },
    {
      "_id" : 3,
      "host" : "localhost:27020",
      "priority" : 1
    }
  ]
}

mapA = function() {
  emit({
    company: this.company
  }, {
    open: this.open,
    close: this.close
  })
}

reduceA = function(key, values) {
    var hCount = 0;
    var lCount = 0;
    for(i = 0; i < values.length; i++){
      if(values[i].close > values[i].open) {
        hCount++;
      }
      else if(values[i].close < values[i].open) {
        lCount++;
      }
    }

    return {timesHigher: hCount, timesLower: lCount};
}

results = db.runCommand({
  mapReduce: 'stocks',
  map: mapA,
  reduce: reduceA,
  query: {date: /2016/},
  out: 'stocks.reducedA'
});

mapB = function() {
  emit({
    company: this.company
  }, {
    date: new Date(this.date),
    close: this.close
  })
}

reduceB = function(key, values) {
  var q1Count = 0;
  var q2Count = 0;
  var q3Count = 0;
  var q4Count = 0;

  var q1 = 0;
  var q2 = 0;
  var q3 = 0;
  var q4 = 0;

  for(i = 0; i < values.length; i++) {
    if(values[i].date.getMonth() <= 3 && values[i].date.getMonth() >= 1) {
      q1Count++;
      q1 += values[i].close;
    }
    else if(values[i].date.getMonth() >= 4 && values[i].date.getMonth() <= 6) {
      q2Count++;
      q2 += values[i].close;
    }
    else if(values[i].date.getMonth() >= 7 && values[i].date.getMonth() <= 9) {
      q3Count++;
      q3 += values[i].close;
    }
    else if(values[i].date.getMonth() >= 10 && values[i].date.getMonth() <= 12) {
      q4Count++;
      q4 += values[i].close;
    }
  }
  q1 /= q1Count;
  q2 /= q2Count;
  q3 /= q3Count;
  q4 /= q4Count;

  return {Quarter1: q1, Quarter2: q2, Quarter3: q3, Quarter4: q4};
}

resultB = db.runCommand({
  mapReduce: 'stocks',
  map: mapB,
  reduce: reduceB,
  query: {date: /2016/},
  out: 'stocks.reducedB'
})

mapC = function() {
  emit({
    company: this.company,
    price: Math.round(this.close - this.open)
  }, {
    count: 1
  })
}

reduceC = function(key, values) {
  var count = 0;
  for(i = 0; i < values.length; i++){
    count += values[i].count;
  }

  return{count: count};
}

resultC = db.runCommand({
  mapReduce: 'stocks',
  map: mapC,
  reduce: reduceC,
  out: 'stocks.reducedC'
})
