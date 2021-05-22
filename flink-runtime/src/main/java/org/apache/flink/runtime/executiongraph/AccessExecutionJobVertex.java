package org.apache.flink.runtime.executiongraph;

import org.apache.flink.runtime.accumulators.StringifiedAccumulatorResult;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.jobgraph.JobVertexID;

/**
 * Common interface for the runtime {@link ExecutionJobVertex} and {@link ArchivedExecutionJobVertex}.
 */
public interface AccessExecutionJobVertex {
	/**
	 * Returns the name for this job vertex.
	 */
	String getName();

	/**
	 * Returns the parallelism for this job vertex.
	 */
	int getParallelism();

	/**
	 * Returns the max parallelism for this job vertex.
	 */
	int getMaxParallelism();

	/**
	 * Returns the resource profile for this job vertex.
	 */
	ResourceProfile getResourceProfile();

	/**
	 * Returns the {@link JobVertexID} for this job vertex.
	 */
	JobVertexID getJobVertexId();

	/**
	 * Returns all execution vertices for this job vertex.
	 */
	AccessExecutionVertex[] getTaskVertices();

	/**
	 * Returns the aggregated {@link ExecutionState} for this job vertex.
	 */
	ExecutionState getAggregateState();

	/**
	 * Returns the aggregated user-defined accumulators as strings.
	 */
	StringifiedAccumulatorResult[] getAggregatedUserAccumulatorsStringified();

}
