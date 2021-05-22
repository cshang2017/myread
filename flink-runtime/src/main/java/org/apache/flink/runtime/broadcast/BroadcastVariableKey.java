package org.apache.flink.runtime.broadcast;

import org.apache.flink.runtime.jobgraph.JobVertexID;

import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * An identifier for a {@link BroadcastVariableMaterialization} based on the task's {@link JobVertexID}, broadcast
 * variable name and iteration superstep.
 */
@Getter
@AllArgsConstructor
public class BroadcastVariableKey {

	private final JobVertexID vertexId;

	private final String name;

	private final int superstep;

}
