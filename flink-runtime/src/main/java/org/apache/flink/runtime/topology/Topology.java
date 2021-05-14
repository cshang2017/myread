package org.apache.flink.runtime.topology;

/**
 * Extends the {@link BaseTopology} by pipelined regions.
 */
public interface Topology<VID extends VertexID, RID extends ResultID,
	V extends Vertex<VID, RID, V, R>, R extends Result<VID, RID, V, R>,
	PR extends PipelinedRegion<VID, RID, V, R>>
	extends BaseTopology<VID, RID, V, R> {

	/**
	 * Returns all pipelined regions in this topology.
	 *
	 * @return Iterable over pipelined regions in this topology
	 */
	default Iterable<? extends PR> getAllPipelinedRegions() {
		throw new UnsupportedOperationException();
	}

	/**
	 * The pipelined region for a specified vertex.
	 *
	 * @param vertexId the vertex id identifying the vertex for which the
	 *                 pipelined region should be returned
	 * @return the pipelined region of the vertex
	 * @throws IllegalArgumentException if there is no vertex in this topology
	 *                                  with the specified vertex id
	 */
	default PR getPipelinedRegionOfVertex(VID vertexId) {
		throw new UnsupportedOperationException();
	}
}
