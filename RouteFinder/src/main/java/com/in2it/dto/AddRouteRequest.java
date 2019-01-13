package com.in2it.dto;

public class AddRouteRequest {
	private String source;
	private String destination;
	private float distance;

	public String getSource() {
		return source;
	}

	public String getDestination() {
		return destination;
	}

	public float getDistance() {
		return distance;
	}

	public void setSource(String source) {
		this.source = source;
	}

	public void setDestination(String destination) {
		this.destination = destination;
	}

	public void setDistance(float distance) {
		this.distance = distance;
	}

}
