package com.in2it.dto;

import java.util.List;

import com.in2it.model.RouteEdge;

public class FindRouteResponse {
	private List<RouteEdge> routePlannets;
	private float totalDistance;

	public float getTotalDistance() {
		return totalDistance;
	}
	public void setTotalDistance(float totalDistance) {
		this.totalDistance = totalDistance;
	}
	public List<RouteEdge> getRoutePlannets() {
		return routePlannets;
	}
	public void setRoutePlannets(List<RouteEdge> routePlannets) {
		this.routePlannets = routePlannets;
	}

}
