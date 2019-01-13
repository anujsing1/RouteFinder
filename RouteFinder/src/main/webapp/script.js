"use strict";

angular
  .module("submitExample", [])
  .controller("SubmitController", function(
    $scope,
    $rootScope,
    $compile,
    locationService
  ) {

      $scope.show=false;
      $scope.routePlannets = [];
      $scope.sourceValue = '';
      $scope.destinationValue = '';
      $scope.routePlannets=[];

      $scope.load = function() {
          locationService.getPlanets().then(function (result) {
              $scope.sourceDestination=result.data;
          });
      }

      $scope.submit = function() {
          locationService.findRoute($scope.sourceValue, $scope.destinationValue).then(function (result) {
              $scope.routePlannets=result.data.routePlannets;
              $scope.totalDistance=result.data.totalDistance;
              $scope.show=true;
          });
      };
  });

angular.module("submitExample").service("locationService", function($http) {
  var service = {};

  service.findRoute = function(src, des) {
    var url = "/route/findRoute?source="+src+"&destination="+des;
    return $http.get(url);
  };

  service.getPlanets = function() {
        var url = "/route/getPlanets";
        return $http.get(url);
  };
  return service;
});