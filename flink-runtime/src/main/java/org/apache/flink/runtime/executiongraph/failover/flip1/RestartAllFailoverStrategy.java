

package org.apache.flink.runtime.executiongraph.failover.flip1;

import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.runtime.scheduler.strategy.SchedulingExecutionVertex;
import org.apache.flink.runtime.scheduler.strategy.SchedulingTopology;
import org.apache.flink.util.IterableUtils;

import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A failover strategy that proposes to restart all vertices when a vertex fails.
 */
public class RestartAllFailoverStrategy implements FailoverStrategy {

	private final SchedulingTopology topology;

	public RestartAllFailoverStrategy(final SchedulingTopology topology) {
		this.topology = checkNotNull(topology);
	}

	/**
	 * Returns all vertices on any task failure.
	 *
	 * @param executionVertexId ID of the failed task
	 * @param cause cause of the failure
	 * @return set of IDs of vertices to restart
	 */
	@Override
	public Set<ExecutionVertexID> getTasksNeedingRestart(ExecutionVertexID executionVertexId, Throwable cause) {
		return IterableUtils.toStream(topology.getVertices())
			.map(SchedulingExecutionVertex::getId)
			.collect(Collectors.toSet());
	}

	/**
	 * The factory to instantiate {@link RestartAllFailoverStrategy}.
	 */
	public static class Factory implements FailoverStrategy.Factory {

		@Override
		public FailoverStrategy create(
				final SchedulingTopology topology,
				final ResultPartitionAvailabilityChecker resultPartitionAvailabilityChecker) {

			return new RestartAllFailoverStrategy(topology);
		}
	}
}
