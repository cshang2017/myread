package org.apache.flink.runtime.jobgraph.topology;

import org.apache.flink.runtime.jobgraph.IntermediateDataSet;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.JobEdge;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;

import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Default implementation of {@link LogicalVertex}.
 * It is an adapter of {@link JobVertex}.
 */
public class DefaultLogicalVertex implements LogicalVertex<DefaultLogicalVertex, DefaultLogicalResult> {

	private final JobVertex jobVertex;

	private final Function<IntermediateDataSetID, DefaultLogicalResult> resultRetriever;

	DefaultLogicalVertex(
			final JobVertex jobVertex,
			final Function<IntermediateDataSetID, DefaultLogicalResult> resultRetriever) {

		this.jobVertex = checkNotNull(jobVertex);
		this.resultRetriever = checkNotNull(resultRetriever);
	}

	@Override
	public JobVertexID getId() {
		return jobVertex.getID();
	}

	@Override
	public Iterable<DefaultLogicalResult> getConsumedResults() {
		return jobVertex.getInputs().stream()
			.map(JobEdge::getSource)
			.map(IntermediateDataSet::getId)
			.map(resultRetriever)
			.collect(Collectors.toList());
	}

	@Override
	public Iterable<DefaultLogicalResult> getProducedResults() {
		return jobVertex.getProducedDataSets().stream()
			.map(IntermediateDataSet::getId)
			.map(resultRetriever)
			.collect(Collectors.toList());
	}
}
