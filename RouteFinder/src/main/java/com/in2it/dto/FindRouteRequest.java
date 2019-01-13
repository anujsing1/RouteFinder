package com.in2it.dto;

public class FindRouteRequest {
	private String source;
	private String destination;
	private Boolean isTrafficIncluded;

	public String getSource() {
		return source;
	}

	public String getDestination() {
		return destination;
	}

	public Boolean isTrafficIncluded() {
		return isTrafficIncluded;
	}

	public void setSource(String source) {
		this.source = source;
	}

	public void setDestination(String destination) {
		this.destination = destination;
	}

	public void setTrafficIncluded(Boolean isTrafficIncluded) {
		this.isTrafficIncluded = isTrafficIncluded;
	}

}
