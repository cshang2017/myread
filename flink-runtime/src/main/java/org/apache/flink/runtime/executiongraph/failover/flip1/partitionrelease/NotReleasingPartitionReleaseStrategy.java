

package org.apache.flink.runtime.executiongraph.failover.flip1.partitionrelease;

import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.runtime.scheduler.strategy.SchedulingTopology;

import java.util.Collections;
import java.util.List;

/**
 * Does not release intermediate result partitions during job execution. Relies on partitions being
 * released at the end of the job.
 */
public class NotReleasingPartitionReleaseStrategy implements PartitionReleaseStrategy {

	@Override
	public List<IntermediateResultPartitionID> vertexFinished(final ExecutionVertexID finishedVertex) {
		return Collections.emptyList();
	}

	@Override
	public void vertexUnfinished(final ExecutionVertexID executionVertexID) {
	}

	/**
	 * Factory for {@link NotReleasingPartitionReleaseStrategy}.
	 */
	public static class Factory implements PartitionReleaseStrategy.Factory {

		@Override
		public PartitionReleaseStrategy createInstance(final SchedulingTopology schedulingStrategy) {
			return new NotReleasingPartitionReleaseStrategy();
		}
	}

}
