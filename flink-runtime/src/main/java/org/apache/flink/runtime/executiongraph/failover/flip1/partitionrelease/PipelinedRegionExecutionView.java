package org.apache.flink.runtime.executiongraph.failover.flip1.partitionrelease;

import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.runtime.scheduler.strategy.SchedulingExecutionVertex;
import org.apache.flink.runtime.scheduler.strategy.SchedulingPipelinedRegion;
import org.apache.flink.util.IterableUtils;

import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Provides a virtual execution state of a {@link SchedulingPipelinedRegion}.
 *
 * <p>A pipelined region can be either finished or unfinished. It is finished iff. all its
 * executions have reached the finished state.
 */
class PipelinedRegionExecutionView {

	private final SchedulingPipelinedRegion pipelinedRegion;

	private final Set<ExecutionVertexID> unfinishedVertices;

	PipelinedRegionExecutionView(final SchedulingPipelinedRegion pipelinedRegion) {
		this.pipelinedRegion = checkNotNull(pipelinedRegion);
		this.unfinishedVertices = IterableUtils.toStream(pipelinedRegion.getVertices())
			.map(SchedulingExecutionVertex::getId)
			.collect(Collectors.toSet());
	}

	public boolean isFinished() {
		return unfinishedVertices.isEmpty();
	}

	public void vertexFinished(final ExecutionVertexID executionVertexId) {
		assertVertexInRegion(executionVertexId);
		unfinishedVertices.remove(executionVertexId);
	}

	public void vertexUnfinished(final ExecutionVertexID executionVertexId) {
		assertVertexInRegion(executionVertexId);
		unfinishedVertices.add(executionVertexId);
	}

	private void assertVertexInRegion(final ExecutionVertexID executionVertexId) {
		pipelinedRegion.getVertex(executionVertexId);
	}
}
