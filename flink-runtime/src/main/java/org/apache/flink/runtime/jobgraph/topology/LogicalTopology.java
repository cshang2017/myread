package org.apache.flink.runtime.jobgraph.topology;

import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.topology.Topology;

/**
 * Represents a logical topology, i.e. {@link JobGraph}.
 */
public interface LogicalTopology<V extends LogicalVertex<V, R>, R extends LogicalResult<V, R>>
	extends Topology<JobVertexID, IntermediateDataSetID, V, R, LogicalPipelinedRegion<V, R>> {
}
