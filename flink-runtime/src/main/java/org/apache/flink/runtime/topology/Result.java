package org.apache.flink.runtime.topology;

import org.apache.flink.runtime.io.network.partition.ResultPartitionType;

/**
 * Represents a data set produced by a {@link Vertex}
 * Each result is produced by one {@link Vertex}.
 * Each result can be consumed by multiple {@link Vertex}.
 */
public interface Result<VID extends VertexID, RID extends ResultID,
	V extends Vertex<VID, RID, V, R>, R extends Result<VID, RID, V, R>> {

	RID getId();

	ResultPartitionType getResultType();

	V getProducer();

	Iterable<? extends V> getConsumers();
}
