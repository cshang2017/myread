package org.apache.flink.runtime.jobgraph.topology;

import org.apache.flink.runtime.jobgraph.IntermediateDataSet;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.topology.Result;

/**
 * Represents a data set produced by a {@link LogicalVertex}, i.e. {@link IntermediateDataSet}.
 */
public interface LogicalResult<V extends LogicalVertex<V, R>, R extends LogicalResult<V, R>>
	extends Result<JobVertexID, IntermediateDataSetID, V, R> {
}
