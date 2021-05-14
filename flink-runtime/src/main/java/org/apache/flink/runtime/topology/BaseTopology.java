package org.apache.flink.runtime.topology;

/**
 * Base topology for all logical and execution topologies.
 * A topology consists of {@link Vertex} and {@link Result}.
 */
public interface BaseTopology<VID extends VertexID, RID extends ResultID,
	V extends Vertex<VID, RID, V, R>, R extends Result<VID, RID, V, R>> {

	/**
	 * Returns an iterable over all vertices, topologically sorted.
	 *
	 * @return topologically sorted iterable over all vertices
	 */
	Iterable<? extends V> getVertices();

	/**
	 * Returns whether the topology contains co-location constraints.
	 * Co-location constraints are currently used for iterations.
	 *
	 * @return whether the topology contains co-location constraints
	 */
	boolean containsCoLocationConstraints();
}
