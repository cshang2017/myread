package org.apache.flink.runtime.jobgraph.topology;

import org.apache.flink.runtime.executiongraph.failover.flip1.PipelinedRegionComputeUtil;
import org.apache.flink.runtime.jobgraph.IntermediateDataSet;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.util.IterableUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Default implementation of {@link LogicalTopology}.
 * It is an adapter of {@link JobGraph}.
 */
public class DefaultLogicalTopology implements LogicalTopology<DefaultLogicalVertex, DefaultLogicalResult> {

	private final boolean containsCoLocationConstraints;

	private final List<DefaultLogicalVertex> verticesSorted;

	private final Map<JobVertexID, DefaultLogicalVertex> idToVertexMap;

	private final Map<IntermediateDataSetID, DefaultLogicalResult> idToResultMap;

	public DefaultLogicalTopology(final JobGraph jobGraph) {
		checkNotNull(jobGraph);

		this.containsCoLocationConstraints = IterableUtils.toStream(jobGraph.getVertices())
			.map(JobVertex::getCoLocationGroup)
			.anyMatch(Objects::nonNull);

		this.verticesSorted = new ArrayList<>(jobGraph.getNumberOfVertices());
		this.idToVertexMap = new HashMap<>();
		this.idToResultMap = new HashMap<>();

		buildVerticesAndResults(jobGraph);
	}

	private void buildVerticesAndResults(final JobGraph jobGraph) {
		final Function<JobVertexID, DefaultLogicalVertex> vertexRetriever = this::getVertex;
		final Function<IntermediateDataSetID, DefaultLogicalResult> resultRetriever = this::getResult;

		for (JobVertex jobVertex : jobGraph.getVerticesSortedTopologicallyFromSources()) {
			final DefaultLogicalVertex logicalVertex = new DefaultLogicalVertex(jobVertex, resultRetriever);
			this.verticesSorted.add(logicalVertex);
			this.idToVertexMap.put(logicalVertex.getId(), logicalVertex);

			for (IntermediateDataSet intermediateDataSet : jobVertex.getProducedDataSets()) {
				final DefaultLogicalResult logicalResult = new DefaultLogicalResult(intermediateDataSet, vertexRetriever);
				idToResultMap.put(logicalResult.getId(), logicalResult);
			}
		}
	}

	@Override
	public Iterable<DefaultLogicalVertex> getVertices() {
		return verticesSorted;
	}

	@Override
	public boolean containsCoLocationConstraints() {
		return containsCoLocationConstraints;
	}

	private DefaultLogicalVertex getVertex(final JobVertexID vertexId) {
		return Optional.ofNullable(idToVertexMap.get(vertexId))
			.orElseThrow(() -> new IllegalArgumentException("can not find vertex: " + vertexId));
	}

	private DefaultLogicalResult getResult(final IntermediateDataSetID resultId) {
		return Optional.ofNullable(idToResultMap.get(resultId))
			.orElseThrow(() -> new IllegalArgumentException("can not find result: " + resultId));
	}

	public Set<DefaultLogicalPipelinedRegion> getLogicalPipelinedRegions() {
		final Set<Set<DefaultLogicalVertex>> regionsRaw = PipelinedRegionComputeUtil.computePipelinedRegions(this);

		final Set<DefaultLogicalPipelinedRegion> regions = new HashSet<>();
		for (Set<DefaultLogicalVertex> regionVertices : regionsRaw) {
			regions.add(new DefaultLogicalPipelinedRegion(regionVertices));
		}
		return regions;
	}
}
