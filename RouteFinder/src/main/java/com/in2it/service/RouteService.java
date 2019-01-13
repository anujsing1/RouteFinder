package com.in2it.service;

import java.util.Set;

import com.in2it.dto.AddRouteRequest;
import com.in2it.dto.FindRouteRequest;
import com.in2it.dto.FindRouteResponse;
import com.in2it.dto.UpdateRouteRequest;

public interface RouteService {
	/**
	 * @param request
	 * @return
	 */
	FindRouteResponse findRoute(FindRouteRequest request);

	/**
	 * @param request
	 */
	void updateRoute(UpdateRouteRequest request);

	/**
	 * @param request
	 */
	void addRoute(AddRouteRequest request);

	/**
	 * @return
	 */
	Set<String> getTotalPlanet();
}
