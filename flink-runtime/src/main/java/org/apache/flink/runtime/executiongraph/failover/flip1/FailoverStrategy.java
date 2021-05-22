package org.apache.flink.runtime.executiongraph.failover.flip1;

import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.runtime.scheduler.strategy.SchedulingTopology;

import java.util.Set;

/**
 * New interface for failover strategies.
 */
public interface FailoverStrategy {

	/**
	 * Returns a set of IDs corresponding to the set of vertices that should be restarted.
	 *
	 * @param executionVertexId ID of the failed task
	 * @param cause cause of the failure
	 * @return set of IDs of vertices to restart
	 */
	Set<ExecutionVertexID> getTasksNeedingRestart(ExecutionVertexID executionVertexId, Throwable cause);

	// ------------------------------------------------------------------------
	//  factory
	// ------------------------------------------------------------------------

	/**
	 * The factory to instantiate {@link FailoverStrategy}.
	 */
	interface Factory {

		/**
		 * Instantiates the {@link FailoverStrategy}.
		 *
		 * @param topology of the graph to failover
		 * @param resultPartitionAvailabilityChecker to check whether a result partition is available
		 * @return The instantiated failover strategy.
		 */
		FailoverStrategy create(
			SchedulingTopology topology,
			ResultPartitionAvailabilityChecker resultPartitionAvailabilityChecker);
	}
}
