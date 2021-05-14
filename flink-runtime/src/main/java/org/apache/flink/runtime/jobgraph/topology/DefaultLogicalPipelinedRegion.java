package org.apache.flink.runtime.jobgraph.topology;

import org.apache.flink.runtime.jobgraph.JobVertexID;

import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Set of {@link LogicalVertex} that are connected through pipelined {@link LogicalResult}.
 */
public class DefaultLogicalPipelinedRegion {

	private final Set<JobVertexID> vertexIDs;

	public DefaultLogicalPipelinedRegion(final Set<? extends LogicalVertex<?, ?>> logicalVertices) {
		checkNotNull(logicalVertices);

		this.vertexIDs = logicalVertices.stream()
			.map(LogicalVertex::getId)
			.collect(Collectors.toSet());
	}

	public Set<JobVertexID> getVertexIDs() {
		return vertexIDs;
	}

	@Override
	public String toString() {
		return "DefaultLogicalPipelinedRegion{" +
			"vertexIDs=" + vertexIDs +
			'}';
	}
}
