package com.in2it.controller;

import java.util.Set;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;

import com.in2it.dto.AddRouteRequest;
import com.in2it.dto.FindRouteRequest;
import com.in2it.dto.FindRouteResponse;
import com.in2it.dto.UpdateRouteRequest;
import com.in2it.service.RouteService;

/**
 * @author Shiv This class will expose methods to outside world
 */
@Controller
@RequestMapping("/route")
public class RouteServiceController {
	@Autowired
	private RouteService routeService;

	/**
	 * This method will take source, destination and isTraffic included flag in
	 * input And return shortest Path
	 * 
	 * @param request
	 * @return
	 */
	@RequestMapping(value = "/findRoute", method = RequestMethod.GET)
	public ResponseEntity<FindRouteResponse> findRute(FindRouteRequest request) {
		FindRouteResponse response = routeService.findRoute(request);
		return new ResponseEntity<FindRouteResponse>(response, HttpStatus.OK);
	}

	/**
	 * This method will take source, destination planet and distance between
	 * them in input If source planet not found then it will not add this.Will
	 * support this feature in the future To create new graph if source not
	 * found
	 * 
	 * @param request
	 */
	@RequestMapping(value = "/addRoute", method = RequestMethod.POST)
	public ResponseEntity<String> addRoute(AddRouteRequest request) {
		routeService.addRoute(request);
		return new ResponseEntity<String>("Added successfully", HttpStatus.OK);
	}

	/**
	 * This method take source, destination planet and distance between them
	 * that need to updated
	 * 
	 * @param request
	 */
	@RequestMapping(value = "/updateRoute", method = RequestMethod.PUT)
	public ResponseEntity<String> updateRoute(UpdateRouteRequest request) {
		routeService.updateRoute(request);
		return new ResponseEntity<String>("Updated successfully", HttpStatus.OK);
	}

	/**
	 * This method will return total number of planets in glaxy. Currently
	 * supporting only on graph/glaxy at any point so not taking graph/glaxy Id
	 * as input
	 * 
	 * @return
	 */
	@RequestMapping(value = "/getPlanets", method = RequestMethod.GET)
	public ResponseEntity<Set<String>> getTotalPlanets() {
		return new ResponseEntity<Set<String>>(routeService.getTotalPlanet(), HttpStatus.OK);
	}
}
