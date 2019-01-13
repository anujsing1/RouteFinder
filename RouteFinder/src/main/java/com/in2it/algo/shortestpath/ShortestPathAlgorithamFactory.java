package com.in2it.algo.shortestpath;

/**
 * This factory class will use to return specific algoritham instance based on isTraficIncluded flag
 * @author Shiv
 *
 */
public class ShortestPathAlgorithamFactory {
	/**
	 * In future will support creating instance of ShortestPathAlgoritham based on given flag
	 * @param isTraficIncluded
	 * @return
	 */
	public static ShortestPathAlgoritham getInstance(Boolean isTraficIncluded){
		return new WeightedShortesPath();
	}
}
