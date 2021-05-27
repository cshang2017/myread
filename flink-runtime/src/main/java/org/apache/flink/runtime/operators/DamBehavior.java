package org.apache.flink.runtime.operators;

/**
 * Enumeration for the different dam behaviors of an algorithm or a driver
 * strategy. The dam behavior describes whether records pass through the
 * algorithm (no dam), whether all records are collected before the first
 * is returned (full dam) or whether a certain large amount is collected before
 * the algorithm returns records. 
 */
public enum DamBehavior {
	
	/**
	 * Constant indicating that the algorithm does not come with any form of dam
	 * and records pass through in a pipelined fashion.
	 */
	PIPELINED,
	
	/**
	 * Constant indicating that the algorithm materialized (some) records, but may
	 * return records before all records are read.
	 */
	MATERIALIZING,
	
	/**
	 * Constant indicating that the algorithm collects all records before returning any.
	 */
	FULL_DAM;
	
	/**
	 * Checks whether this enumeration represents some form of materialization,
	 * either with a full dam or without.
	 * 
	 * @return True, if this enumeration constant represents a materializing behavior,
	 *         false otherwise.
	 */
	public boolean isMaterializing() {
		return this != PIPELINED;
	}
}
