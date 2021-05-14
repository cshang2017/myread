package org.apache.flink.runtime.jobgraph;

import org.apache.flink.runtime.executiongraph.ExecutionVertex;

/**
 * A distribution pattern determines, which sub tasks of a producing task are connected to which
 * consuming sub tasks.
 */
public enum DistributionPattern {

	/**
	 * Each producing sub task is connected to each sub task of the consuming task.
	 * <p>
	 * {@link ExecutionVertex#connectAllToAll(org.apache.flink.runtime.executiongraph.IntermediateResultPartition[], int)}
	 */
	ALL_TO_ALL,

	/**
	 * Each producing sub task is connected to one or more subtask(s) of the consuming task.
	 * <p>
	 * {@link ExecutionVertex#connectPointwise(org.apache.flink.runtime.executiongraph.IntermediateResultPartition[], int)}
	 */
	POINTWISE
}
