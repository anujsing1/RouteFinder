package com.in2it.model;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import javax.persistence.Table;

@Entity
@Table(name="RouteEdge")
public class RouteEdge {
	@Id
	@GeneratedValue
	int routeId;
	private String source;
	private String detination;
	@Column(precision=10, scale=2 , columnDefinition="DECIMAL(10,2)")
	private float distance;
	public RouteEdge(){};
	public RouteEdge(String source, String detination, float distance) {
		super();
		this.source = source;
		this.detination = detination;
		this.distance = distance;
	}

	@ManyToOne
	@JoinColumn(name="Plannet_Id")
	private Planet planet;
	public String getSource() {
		return source;
	}

	public String getDetination() {
		return detination;
	}

	public float getDistance() {
		return distance;
	}

	public void setSource(String source) {
		this.source = source;
	}

	public void setDetination(String detination) {
		this.detination = detination;
	}

	public void setDistance(float distance) {
		this.distance = distance;
	}

	public int getRouteId() {
		return routeId;
	}

	public void setRouteId(int routeId) {
		this.routeId = routeId;
	}
	public Planet getPlanet() {
		return planet;
	}
	public void setPlanet(Planet planet) {
		this.planet = planet;
	}

}
