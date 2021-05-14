package org.apache.flink.runtime.jobgraph.topology;

import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.topology.Vertex;

/**
 * Represents a vertex in {@link LogicalTopology}, i.e. {@link JobVertex}.
 */
public interface LogicalVertex<V extends LogicalVertex<V, R>, R extends LogicalResult<V, R>>
	extends Vertex<JobVertexID, IntermediateDataSetID, V, R> {
}
