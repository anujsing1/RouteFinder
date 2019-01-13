package com.in2it.service;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.in2it.algo.shortestpath.ShortestPathAlgoritham;
import com.in2it.algo.shortestpath.ShortestPathAlgorithamFactory;
import com.in2it.dao.RouteDao;
import com.in2it.dto.AddRouteRequest;
import com.in2it.dto.FindRouteRequest;
import com.in2it.dto.FindRouteResponse;
import com.in2it.dto.UpdateRouteRequest;
import com.in2it.model.PlanetGraph;
import com.in2it.model.RouteEdge;

/**
 * Not added support to store multiple graph in the system
 * @author Shiv
 *
 */
@Service
public class RouteServiceImpl implements RouteService {
	@Autowired
	private RouteDao routeDao;
	@Override
	/**
	 * This method will take source, destination and isTraffic included flag in input
	 * And return shortest Path 
	 * @param request
	 * @return
	 */
	public FindRouteResponse findRoute(FindRouteRequest request) {
		if (request == null) {
			return null;
		}
		FindRouteResponse response = new FindRouteResponse();
		if(request.getSource().equalsIgnoreCase(request.getDestination())) {
			List<RouteEdge> routePlannets= new ArrayList<>();
			RouteEdge routeEdge = new RouteEdge(request.getSource(), request.getDestination(),0);
			routeEdge.setRouteId(-1);
			routePlannets.add(routeEdge);
			response.setRoutePlannets(routePlannets);
			response.setTotalDistance(0);
			return response;
		}
		PlanetGraph graph = routeDao.getGraph();
		ShortestPathAlgoritham algoritham = ShortestPathAlgorithamFactory.getInstance(request.isTrafficIncluded());
		response = algoritham.shortestPath(graph, request.getSource(), request.getDestination());
		return response;
	}

	@Override
	/**
	 * This method take source, destination planet and distance between them that need to updated
	 * @param request
	 */
	public void updateRoute(UpdateRouteRequest request) {
		if (request != null) {
			RouteEdge edge = new RouteEdge(request.getSource(), request.getDestination(), request.getDistance());
			routeDao.updateRoute(edge);
		}

	}

	@Override
	/**
	 * This method will take source, destination planet and distance between them in input
	 * If source planet not found then it will not add this.Will support this feature in the future
	 * To create new graph if source not found
	 * @param request
	 */
	public void addRoute(AddRouteRequest request) {
		if (request != null) {
			RouteEdge edge = new RouteEdge(request.getSource(), request.getDestination(), request.getDistance());
			routeDao.addRoute(edge);
		}
	}

	@Override
	/**
	 * This method will return total number of planets in glaxy.
	 * Currently supporting only on graph/glaxy at any point so not taking graph/glaxy Id as input
	 * @return
	 */
	public Set<String> getTotalPlanet() {
		return routeDao.getTotalPlanet();
	}

}
