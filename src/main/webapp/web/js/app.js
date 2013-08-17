angular.module('spark', []).

directive('moodChart', function() {
  return {
    restrict: 'A',
    link: function(scope, element) {
      var m = [20, 20, 30, 20],
          w = 960 - m[1] - m[3],
          h = 500 - m[0] - m[2];
      var color = d3.scale.category20();

      var start = new Date().getTime();
      var x = d3.scale.linear()
                      .domain([start-10, start+(10*60*1000)+10])
                      .range([0, w - 60]);

      var y = d3.scale.linear()
                      .domain([-150, 150])
                      .range([h, 0]);

      //initialize the chart
      var svg = d3.select("body")
                    .append("svg:svg")
                      .attr("width", w + m[1] + m[3])
                      .attr("height", h + m[0] + m[2])
                    .append("svg:g")
                      .attr("transform", "translate("+m[3]+","+m[0]+")");
      var line = d3.svg.line()
          .interpolate("basis")
          .x(function(d) {
            return x(d.time);
            //return d.time;
          })
          .y(function(d) {
            return y(d.score);
          });

      scope.$watch('timelines', function (ts) {
        var timelines = d3.entries(ts);
        var domain = [scope.maxTime-(10*60*1000-10), scope.maxTime+10];
        x.domain(domain);

        angular.forEach(timelines, function(t) {
          t.value = t.value.filter(function(i) { return x(i.time) >= 0});
        });

        var lines = svg.selectAll(".line")
                        .data(timelines)
                          .attr("d", function(d) {
                            return line(d.value);
                          });
        lines.enter()
              .append("svg:path")
              .attr("class", "line");
              //.attr("d", function(d) {
              //  return line(d.value);
              //});
      })

    }
  }
}).
controller('SparkCtrl', function ($scope, $http) {
  $scope.start = function() {
    $http
      .get("/start")
      .success(function() {console.log("spark started")})
      .error(function() {console.error("spark not started")})
  };
  $scope.stop = function() {
    $http
      .get("/stop")
      .success(function() {console.log("spark stopped")})
      .error(function() {console.error("spark not stopped")})
  };
}).
controller('StockCtrl', function ($scope) {
  $scope.stocks = [
    {id:'GOOG'},
    {id:'AAPL'}];

  $scope.addStock = function() {
    $scope.stocks.push({id:$scope.stockId});
    $scope.stockId = '';
  };
}).
controller('ResultsCtrl', function($scope, $http, $timeout) {
  $scope.timelines = {};

  var merge = function(container, elements) {
    var result = {};
    angular.forEach(container, function(v, k) {
      result[k] = v;
      if (k in elements) {
        var ls = elements[k].reverse();
        angular.forEach(ls, function(i) {
          result[k].push(i);
        });
        delete elements[k];
      }
    });
    angular.forEach(elements, function(v, k) {
      result[k] = v.reverse();
    });
    return result;
  };

  var poll = function(delay, time) {
    var polling = function() {
      $http.
        get('/after'+(time?"?time="+time:"")).
        success(function(data, status, headers, config) {
          $scope.maxTime = data[1];
          $scope.timelines = merge($scope.timelines, data[0]);

          var newDelay = (delay>5000)?delay-5000:delay;
          poll(newDelay, data[1]+1);
        }).
        error(function(data, status, headers, config) {
          console.error(data);

          var newDelay = (delay<60000)?delay+5000:delay;
          console.log("Will wait for "+newDelay+"ms")
          poll(newDelay, time);
        });
    };
    $timeout(polling, delay);
  };
  poll(5000);
});