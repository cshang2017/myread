package org.apache.flink.runtime.jobgraph.topology;

import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.topology.PipelinedRegion;

/**
 * Pipelined region on logical level, i.e., {@link JobVertex} level.
 */
public interface LogicalPipelinedRegion<V extends LogicalVertex<V, R>, R extends LogicalResult<V, R>> extends PipelinedRegion<JobVertexID, IntermediateDataSetID, V, R> {
}
