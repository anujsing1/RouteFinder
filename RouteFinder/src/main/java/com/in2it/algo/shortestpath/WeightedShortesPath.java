package com.in2it.algo.shortestpath;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.in2it.dto.FindRouteResponse;
import com.in2it.model.Planet;
import com.in2it.model.PlanetGraph;
import com.in2it.model.RouteEdge;

/**
 * This class will return shortest path between given source and destination
 * @author Shiv
 *
 */
public class WeightedShortesPath implements ShortestPathAlgoritham {
	private final List<Planet> nodes = new ArrayList<Planet>();
	private final List<RouteEdge> edges = new ArrayList<RouteEdge>();
	private Set<Planet> settledNodes;
	private Set<Planet> unSettledNodes;
	private Map<Planet, Planet> predecessors;
	private Map<Planet, Double> distance;
	HashMap<String, Planet> mapPlanet = new HashMap<String, Planet>();

	public FindRouteResponse shortestPath(PlanetGraph PlanetGraph, String src, String dest) {	
		for (Planet planet : PlanetGraph.getPlannets()) {
			mapPlanet.put(planet.getName(), planet);
			this.nodes.add(planet);
			edges.addAll(planet.getRouteEdges());
		}
		execute(mapPlanet.get(src.toUpperCase()));
		LinkedList<Planet> planets = getPath(mapPlanet.get(dest.toUpperCase()));
		double totalDistance;
		ArrayList<String> planetName = new ArrayList<String>();
		for (Planet planet : planets) {
			planetName.add(planet.getName());
		}
		totalDistance = getShortestDistance(mapPlanet.get(dest.toUpperCase()));
		List<RouteEdge> finalPath = new ArrayList<>();
		Planet previousPlanet = null;
		for (Planet planet : planets) {
			if (previousPlanet == null) {
				previousPlanet = planet;
				continue;
			}
			if (planet != null) {
				for (RouteEdge edge : previousPlanet.getRouteEdges()) {
					if (planet.getName().equalsIgnoreCase(edge.getDetination())) {
						RouteEdge routeEdge=new RouteEdge(edge.getSource(),edge.getDetination(),edge.getDistance());
						routeEdge.setRouteId(edge.getRouteId());
						finalPath.add(routeEdge);
					}
				}
				previousPlanet = planet;
			}
		}
		FindRouteResponse response = new FindRouteResponse();
		response.setRoutePlannets(finalPath);
		response.setTotalDistance((float) totalDistance);
		return response;
	}

	public void execute(Planet source) {
		settledNodes = new HashSet<Planet>();
		unSettledNodes = new HashSet<Planet>();
		distance = new HashMap<Planet, Double>();
		predecessors = new HashMap<Planet, Planet>();
		distance.put(source, (double) 0);
		unSettledNodes.add(source);
		while (unSettledNodes.size() > 0) {
			Planet node = getMinimum(unSettledNodes);
			settledNodes.add(node);
			unSettledNodes.remove(node);
			findMinimalDistances(node);
		}
	}

	private void findMinimalDistances(Planet node) {
		List<Planet> adjacentNodes = getNeighbors(node);
		for (Planet target : adjacentNodes) {
			if (target != null && Double.compare(getShortestDistance(target),
					getShortestDistance(node) + getDistance(node, target)) > 0) {
				distance.put(target, (double) (getShortestDistance(node) + getDistance(node, target)));
				predecessors.put(target, node);
				unSettledNodes.add(target);
			}
		}

	}

	private double getDistance(Planet node, Planet target) {
		try {
			for (RouteEdge RouteEdge : edges) {
				try {
					if (mapPlanet.get(RouteEdge.getSource().toUpperCase()).equals(node)
							&& mapPlanet.get(RouteEdge.getDetination().toUpperCase()).equals(target)) {
						return RouteEdge.getDistance();
					}
				} catch (Exception e) {

				}
			}
		} finally {

		}
		throw new RuntimeException("Should not happen");
	}

	private List<Planet> getNeighbors(Planet node) {
		List<Planet> neighbors = new ArrayList<Planet>();
		for (RouteEdge RouteEdge : edges) {
			if (mapPlanet.get(RouteEdge.getSource().toUpperCase()).equals(node)
					&& !isSettled(mapPlanet.get(RouteEdge.getDetination().toUpperCase()))) {
				neighbors.add(mapPlanet.get(RouteEdge.getDetination()));
			}
		}
		return neighbors;
	}

	private Planet getMinimum(Set<Planet> Planetes) {
		Planet minimum = null;
		for (Planet Planet : Planetes) {
			if (minimum == null) {
				minimum = Planet;
			} else {
				if (Double.compare(getShortestDistance(Planet), getShortestDistance(minimum)) < 0) {
					minimum = Planet;
				}
			}
		}
		return minimum;
	}

	private boolean isSettled(Planet Planet) {
		return settledNodes.contains(Planet);
	}

	private Double getShortestDistance(Planet destination) {
		Double d = distance.get(destination);
		if (d == null) {
			return Double.MAX_VALUE;
		} else {
			return d;
		}
	}

	/*
	 * This method returns the path from the source to the selected target and
	 * NULL if no path exists
	 */
	public LinkedList<Planet> getPath(Planet target) {
		LinkedList<Planet> path = new LinkedList<Planet>();
		Planet step = target;
		// check if a path exists
		if (predecessors.get(step) == null) {
			return null;
		}
		path.add(step);
		while (predecessors.get(step) != null) {
			step = predecessors.get(step);
			path.add(step);
		}
		// Put it into the correct order
		Collections.reverse(path);
		return path;
	}
}

class HeapNode {
	private String planet;
	float distance;

	public HeapNode(String planet, float distance) {
		super();
		this.planet = planet;
		this.distance = distance;
	}

	public String getPlanet() {
		return planet;
	}

	public float getDistance() {
		return distance;
	}

	public void setPlanet(String planet) {
		this.planet = planet;
	}

	public void setDistance(float distance) {
		this.distance = distance;
	}

}