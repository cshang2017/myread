package org.apache.flink.runtime.executiongraph;

import org.apache.flink.runtime.accumulators.StringifiedAccumulatorResult;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.jobgraph.JobVertexID;

import java.io.Serializable;

import static org.apache.flink.runtime.executiongraph.ExecutionJobVertex.getAggregateJobVertexState;

@Getter
public class ArchivedExecutionJobVertex implements AccessExecutionJobVertex, Serializable {

	private final ArchivedExecutionVertex[] taskVertices;

	private final JobVertexID id;

	private final String name;

	private final int parallelism;

	private final int maxParallelism;

	private final ResourceProfile resourceProfile;

	private final StringifiedAccumulatorResult[] archivedUserAccumulators;

	public ArchivedExecutionJobVertex(ExecutionJobVertex jobVertex) {
		this.taskVertices = new ArchivedExecutionVertex[jobVertex.getTaskVertices().length];
		for (int x = 0; x < taskVertices.length; x++) {
			taskVertices[x] = jobVertex.getTaskVertices()[x].archive();
		}

		archivedUserAccumulators = jobVertex.getAggregatedUserAccumulatorsStringified();

		this.id = jobVertex.getJobVertexId();
		this.name = jobVertex.getJobVertex().getName();
		this.parallelism = jobVertex.getParallelism();
		this.maxParallelism = jobVertex.getMaxParallelism();
		this.resourceProfile = jobVertex.getResourceProfile();
	}

	public ArchivedExecutionJobVertex(
			ArchivedExecutionVertex[] taskVertices,
			JobVertexID id,
			String name,
			int parallelism,
			int maxParallelism,
			ResourceProfile resourceProfile,
			StringifiedAccumulatorResult[] archivedUserAccumulators) {
		this.taskVertices = taskVertices;
		this.id = id;
		this.name = name;
		this.parallelism = parallelism;
		this.maxParallelism = maxParallelism;
		this.resourceProfile = resourceProfile;
		this.archivedUserAccumulators = archivedUserAccumulators;
	}


	@Override
	public ExecutionState getAggregateState() {
		int[] num = new int[ExecutionState.values().length];
		for (ArchivedExecutionVertex vertex : this.taskVertices) {
			num[vertex.getExecutionState().ordinal()]++;
		}

		return getAggregateJobVertexState(num, parallelism);
	}

}
