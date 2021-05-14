

package org.apache.flink.streaming.api.graph;

import org.apache.flink.runtime.jobgraph.JobVertexID;

import java.util.Map;

/**
 * Interface for different implementations of generating hashes over a stream graph.
 */
public interface StreamGraphHasher {

	/**
	 * Returns a map with a hash for each {@link StreamNode} of the {@link
	 * StreamGraph}. The hash is used as the {@link JobVertexID} in order to
	 * identify nodes across job submissions if they didn't change.
	 */
	Map<Integer, byte[]> traverseStreamGraphAndGenerateHashes(StreamGraph streamGraph);
}
