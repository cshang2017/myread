package org.apache.flink.runtime.topology;

/**
 * A pipelined region is a set of vertices connected via pipelined data
 * exchanges.
 *
 * @param <VID> the type of the vertex ids
 * @param <RID> the type of the result ids
 * @param <V> the type of the vertices
 * @param <R> the type of the result
 */
public interface PipelinedRegion<VID extends VertexID, RID extends ResultID,
	V extends Vertex<VID, RID, V, R>, R extends Result<VID, RID, V, R>> {

	/**
	 * Returns vertices that are in this pipelined region.
	 *
	 * @return Iterable over all vertices in this pipelined region
	 */
	Iterable<? extends V> getVertices();

	/**
	 * Returns the vertex with the specified vertex id.
	 *
	 * @param vertexId the vertex id used to look up the vertex
	 * @return the vertex with the specified id
	 * @throws IllegalArgumentException if there is no vertex in this pipelined
	 *                                  region with the specified vertex id
	 */
	V getVertex(VID vertexId);

	/**
	 * Returns the results that this pipelined region consumes.
	 *
	 * @return Iterable over all consumed results
	 */
	Iterable<? extends R> getConsumedResults();
}
