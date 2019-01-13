package com.in2it.dao;

import java.util.Set;

import com.in2it.model.PlanetGraph;
import com.in2it.model.RouteEdge;

public interface RouteDao {
	 /**
	 * @param graph
	 */
	void saveGraph(PlanetGraph graph);
	 /**
	 * @param graphId
	 * @return
	 */
	PlanetGraph getGraph();
	 /**
	 * @param edge
	 * @param graphId
	 */
	void addRoute(RouteEdge edge);
	 /**
	 * @param edge
	 * @param graphId
	 */
	void updateRoute(RouteEdge edge);
	/**
	 * @return
	 */
	Set<String> getTotalPlanet();
}
