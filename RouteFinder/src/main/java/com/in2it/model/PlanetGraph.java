package com.in2it.model;

import java.util.ArrayList;
import java.util.List;

import javax.persistence.CascadeType;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.OneToMany;
import javax.persistence.Table;

import org.hibernate.annotations.Fetch;
import org.hibernate.annotations.FetchMode;

@Entity
@Table(name="PlanetGraph")
public class PlanetGraph {
	@Id
	@GeneratedValue
	private int id;
	@OneToMany(cascade = CascadeType.ALL, fetch = FetchType.EAGER, mappedBy = "planetGraph")
	@Fetch(value = FetchMode.SUBSELECT)
	private List<Planet> plannets;

	public int getId() {
		return id;
	}

	public List<Planet> getPlannets() {
		if (plannets == null) {
			plannets = new ArrayList<>();
		}
		return plannets;
	}

	public void setId(int id) {
		this.id = id;
	}
}
