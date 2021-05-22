

package org.apache.flink.runtime.executiongraph.failover.flip1.partitionrelease;

import org.apache.flink.runtime.executiongraph.IntermediateResultPartition;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.runtime.scheduler.strategy.SchedulingTopology;

import java.util.List;

/**
 * Interface for strategies that decide when to release
 * {@link IntermediateResultPartition IntermediateResultPartitions}.
 */
public interface PartitionReleaseStrategy {

	/**
	 * Calling this method informs the strategy that a vertex finished.
	 *
	 * @param finishedVertex Id of the vertex that finished the execution
	 * @return A list of result partitions that can be released
	 */
	List<IntermediateResultPartitionID> vertexFinished(ExecutionVertexID finishedVertex);

	/**
	 * Calling this method informs the strategy that a vertex is no longer in finished state, e.g.,
	 * when a vertex is re-executed.
	 *
	 * @param executionVertexID Id of the vertex that is no longer in finished state.
	 */
	void vertexUnfinished(ExecutionVertexID executionVertexID);

	/**
	 * Factory for {@link PartitionReleaseStrategy}.
	 */
	interface Factory {
		PartitionReleaseStrategy createInstance(SchedulingTopology schedulingStrategy);
	}
}
