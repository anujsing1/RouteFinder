package com.in2it.loader;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.annotation.PostConstruct;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.in2it.dao.RouteDao;
import com.in2it.model.Planet;
import com.in2it.model.PlanetGraph;
import com.in2it.model.RouteEdge;

/**
 * @author Shiv This class will only parse csv file.
 */
@Component
public class LoadGraphFromFile {
	private static final String FILE_NAME = ".\\src\\main\\resources\\graphInput.csv";
	private Logger logger=LoggerFactory.getLogger(LoadGraphFromFile.class);
	@Autowired
	private RouteDao routeDao;

	public Map<String, List<RouteEdge>> load() {
		Map<String, List<RouteEdge>> graph = new HashMap<>();
		List<String> lines = parseFile();
		if (!lines.isEmpty()) {
			for (String row : lines) {
				String columns[] = row.split(",");
				if (columns.length < 4) {
					continue;
				}
				String source = columns[1];
				String destination = columns[2];
				float distance = stringToFloat(columns[3]);
				// If distance is not a number in file then skip this row.
				if (distance == -1) {
					continue;
				}
				List<RouteEdge> sourceToDestinationEdges = graph.get(source);
				if (sourceToDestinationEdges == null) {
					sourceToDestinationEdges = new ArrayList<>();
				}
				RouteEdge sourceToDestinationRoute = new RouteEdge(source, destination, distance);
				sourceToDestinationEdges.add(sourceToDestinationRoute);
				graph.put(source, sourceToDestinationEdges);
				List<RouteEdge> destinationToSourceEdge = graph.get(destination);
				if (destinationToSourceEdge == null) {
					destinationToSourceEdge = new ArrayList<>();
				}
				RouteEdge destinationToSourceRoute = new RouteEdge(destination, source, distance);
				destinationToSourceEdge.add(destinationToSourceRoute);
				graph.put(destination, destinationToSourceEdge);
			}
		}
		return graph;
	}

	private float stringToFloat(String distanceStr) {
		float distance = -1;
		try {
			distance = Float.parseFloat(distanceStr);
		} catch (Exception e) {
			logger.error(distanceStr + " is not a valid number skipping this row");
		}
		return distance;
	}

	/**
	 * @param fileName
	 * @return
	 */
	private List<String> parseFile() {
		List<String> lines = new ArrayList<>();
		BufferedReader br = null;
		try {
			File inputFile = new File(FILE_NAME);
			InputStream inputFS = new FileInputStream(inputFile);
			br = new BufferedReader(new InputStreamReader(inputFS));
			// skip the header of the csv
			lines = br.lines().skip(1).collect(Collectors.toList());
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} finally {
			if (br != null) {
				try {
					br.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}

		}
		return lines;
	}

	private void saveGraph(Map<String, List<RouteEdge>> graph) {
		if (graph == null || graph.isEmpty()) {
			logger.warn("Graph is empty cant save");
			return;
		}
		PlanetGraph planetGraph = new PlanetGraph();
		Planet planet = null;
		for (Map.Entry<String, List<RouteEdge>> graphEntry : graph.entrySet()) {
			if (graphEntry != null && graphEntry.getValue() != null && graphEntry.getKey() != null) {
				planet = new Planet(graphEntry.getKey());
				for (RouteEdge routeEdge : graphEntry.getValue()) {
					if (routeEdge != null) {
						routeEdge.setPlanet(planet);
						planet.getRouteEdges().add(routeEdge);
					}
				}
				planet.setPlanetGraph(planetGraph);
				planetGraph.getPlannets().add(planet);
			}
		}
		routeDao.saveGraph(planetGraph);
		logger.info("Graph saved successfully!!!!");
	}

	/**
	 * Read csv file build the graph in memory
	 */
	@PostConstruct
	public void init() {
		Map<String, List<RouteEdge>> graph = load();
		saveGraph(graph);
	}
}
