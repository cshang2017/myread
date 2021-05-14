package org.apache.flink.runtime.jobgraph.topology;

import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.IntermediateDataSet;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.JobEdge;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;

import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Default implementation of {@link LogicalResult}.
 * It is an adapter of {@link IntermediateDataSet}.
 */
public class DefaultLogicalResult implements LogicalResult<DefaultLogicalVertex, DefaultLogicalResult> {

	private final IntermediateDataSet intermediateDataSet;

	private final Function<JobVertexID, DefaultLogicalVertex> vertexRetriever;

	DefaultLogicalResult(
			final IntermediateDataSet intermediateDataSet,
			final Function<JobVertexID, DefaultLogicalVertex> vertexRetriever) {

		this.intermediateDataSet = checkNotNull(intermediateDataSet);
		this.vertexRetriever = checkNotNull(vertexRetriever);
	}

	@Override
	public IntermediateDataSetID getId() {
		return intermediateDataSet.getId();
	}

	@Override
	public ResultPartitionType getResultType() {
		return intermediateDataSet.getResultType();
	}

	@Override
	public DefaultLogicalVertex getProducer() {
		return vertexRetriever.apply(intermediateDataSet.getProducer().getID());
	}

	@Override
	public Iterable<DefaultLogicalVertex> getConsumers() {
		return intermediateDataSet.getConsumers().stream()
			.map(JobEdge::getTarget)
			.map(JobVertex::getID)
			.map(vertexRetriever)
			.collect(Collectors.toList());
	}
}
