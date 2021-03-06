angular.module('spark', []).

directive('moodChart', function() {
  return {
    restrict: 'A',
    link: function(scope, element) {
      var m = [20, 20, 60, 30],
          w = 730 - m[1] - m[3],
          h = 450 - m[0] - m[2];
      var color = d3.scale.category20();

      var start = new Date();
      var x = d3.time.scale()
                      .domain([new Date(start.getTime()-(10*60*1000)), start])
                      .range([0, w - 60]);

      var y = d3.scale.linear()
                      .domain([-10, 10])
                      .range([h, 0]);

      var xAxis = d3.svg.axis()
                        .scale(x)
                        .orient("bottom")
                        .ticks(10)
                        .tickFormat(function(d) {
                          d = (x.domain()[1] - d)/1000;
                          var s = "";
                          if (d > 60) {
                            var div = Math.floor(d / 60);
                            s += div+"m ";
                            d = d%60;
                          }
                          return "- "+s+Math.floor(d) + "s";
                        });

      var yAxis = d3.svg.axis()
                        .scale(y)
                        .orient("left")
                        .ticks(20);

      var d3element = d3.select(element.get(0));

      var legend = d3element.select(".legend")
                              .append("ul");

      //initialize the chart
      var svg = d3element
                  .select(".graph")
                    .append("svg:svg")
                      .attr("width", w + m[1] + m[3])
                      .attr("height", h + m[0] + m[2])
                    .append("svg:g")
                      .attr("transform", "translate("+m[3]+","+m[0]+")");

      svg.append("g")
          .attr("class", "x axis")
          .call(xAxis)
          .attr("transform", "translate(0, "+h+")")
          .selectAll("text")
            .attr("transform", "rotate(90, 6,6) translate(30, 0)");

      svg.append("g")
          .attr("class", "y axis")
          .call(yAxis);

      var line = d3.svg.line()
          .interpolate("basis")
          .x(function(d) {
            return x(new Date(d.time));
          })
          .y(function(d) {
            return y(d.score);
          });

      //quick and dirty array of arrays flattener
      var flatten = function flatten(xs, acc) {
                      acc = acc || [];
                      if (xs.length > 0) {
                        xs[0].forEach(function(i) {
                          acc.push(i);
                        });
                        return flatten(xs.slice(1), acc);
                      } else
                        return acc;
                    };


      scope.$watch('timelines', function (ts) {
        var timelines = d3.entries(ts);

        x.domain([new Date(scope.maxTime-(10*60*1000)), new Date(scope.maxTime)])

        var yOldDomain = y.domain();
        var yDomain = d3.extent(flatten(timelines.map(function(i) {return i.value;})), function(i) { return i.score});
        if (yOldDomain[0] != yDomain[0] || yOldDomain[1] != yDomain[1]) {
          y.domain(yDomain);
          svg.select("g.y.axis").call(yAxis);
        }

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
                .attr("class", function(d) { return "line " + d.key})
                .style("stroke", function(d) {return color(d.key);})
                .append("title")
                  .text(function(d) { return d.key });

        var stocks = legend
                      .selectAll(".stock")
                        .data(timelines);
        stocks.enter()
                .append("li")
                  .attr("class", "stock")
                  .text(function(d){return d.key;})
                  .style("color", function(d) {return color(d.key);});
      });
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