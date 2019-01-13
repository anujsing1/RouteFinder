package com.in2it.model;

import java.util.ArrayList;
import java.util.List;

import javax.persistence.CascadeType;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import javax.persistence.OneToMany;

import org.hibernate.annotations.Fetch;
import org.hibernate.annotations.FetchMode;

@Entity
public class Planet {
	@Id
	@GeneratedValue
	private int id;
	private String name;
	@OneToMany(cascade = CascadeType.ALL, fetch = FetchType.EAGER, mappedBy = "planet")
	@Fetch(value = FetchMode.SUBSELECT)
	private List<RouteEdge> routeEdges;
	@ManyToOne
	@JoinColumn(name = "Plannet_Graph_Id")
	private PlanetGraph planetGraph;

	public Planet() {
	}

	public Planet(String name) {
		super();
		this.name = name;
	}

	public String getName() {
		return name;
	}

	public List<RouteEdge> getRouteEdges() {
		if (routeEdges == null) {
			routeEdges = new ArrayList<RouteEdge>();
		}
		return routeEdges;
	}

	public void setName(String name) {
		this.name = name;
	}

	public PlanetGraph getPlanetGraph() {
		return planetGraph;
	}

	public void setPlanetGraph(PlanetGraph planetGraph) {
		this.planetGraph = planetGraph;
	}

	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}

}
