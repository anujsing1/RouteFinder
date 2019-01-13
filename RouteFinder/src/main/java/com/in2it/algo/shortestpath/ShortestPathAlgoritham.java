package com.in2it.algo.shortestpath;

import com.in2it.dto.FindRouteResponse;
import com.in2it.model.PlanetGraph;

public interface ShortestPathAlgoritham {
	FindRouteResponse shortestPath(PlanetGraph graph, String sourcePlanet, String destinationPlanet);
}
