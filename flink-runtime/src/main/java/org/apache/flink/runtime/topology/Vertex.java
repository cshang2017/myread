
package org.apache.flink.runtime.topology;

/**
 * Represents a logical or execution task.
 * Each vertex can consume data from multiple {@link Result}.
 * Each vertex can produce multiple {@link Result}.
 */
public interface Vertex<VID extends VertexID, RID extends ResultID,
	V extends Vertex<VID, RID, V, R>, R extends Result<VID, RID, V, R>> {

	VID getId();

	Iterable<? extends R> getConsumedResults();

	Iterable<? extends R> getProducedResults();
}
