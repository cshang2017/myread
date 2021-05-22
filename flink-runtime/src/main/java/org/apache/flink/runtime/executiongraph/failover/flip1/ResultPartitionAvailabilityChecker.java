package org.apache.flink.runtime.executiongraph.failover.flip1;

import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;

/**
 * This checker helps to query result partition availability.
 */
public interface ResultPartitionAvailabilityChecker {

	/**
	 * Returns whether the given partition is available.
	 *
	 * @param resultPartitionID ID of the result partition to query
	 * @return whether the given partition is available
	 */
	boolean isAvailable(IntermediateResultPartitionID resultPartitionID);
}

