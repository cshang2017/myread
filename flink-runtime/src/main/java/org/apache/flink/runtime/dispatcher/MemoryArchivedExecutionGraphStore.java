package org.apache.flink.runtime.dispatcher;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionGraph;
import org.apache.flink.runtime.messages.webmonitor.JobDetails;
import org.apache.flink.runtime.messages.webmonitor.JobsOverview;
import org.apache.flink.runtime.webmonitor.WebMonitorUtils;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * {@link ArchivedExecutionGraphStore} implementation which stores the {@link ArchivedExecutionGraph}
 * in memory.
 */
public class MemoryArchivedExecutionGraphStore implements ArchivedExecutionGraphStore {

	private final Map<JobID, ArchivedExecutionGraph> serializableExecutionGraphs = new HashMap<>(4);

	@Override
	public int size() {
		return serializableExecutionGraphs.size();
	}

	@Nullable
	@Override
	public ArchivedExecutionGraph get(JobID jobId) {
		return serializableExecutionGraphs.get(jobId);
	}

	@Override
	public void put(ArchivedExecutionGraph serializableExecutionGraph) throws IOException {
		serializableExecutionGraphs.put(serializableExecutionGraph.getJobID(), serializableExecutionGraph);
	}

	@Override
	public JobsOverview getStoredJobsOverview() {
		Collection<JobStatus> allJobStatus = serializableExecutionGraphs.values().stream()
			.map(ArchivedExecutionGraph::getState)
			.collect(Collectors.toList());

		return JobsOverview.create(allJobStatus);
	}

	@Override
	public Collection<JobDetails> getAvailableJobDetails() {
		return serializableExecutionGraphs.values().stream()
			.map(WebMonitorUtils::createDetailsForJob)
			.collect(Collectors.toList());
	}

	@Nullable
	@Override
	public JobDetails getAvailableJobDetails(JobID jobId) {
		final ArchivedExecutionGraph archivedExecutionGraph = serializableExecutionGraphs.get(jobId);

		if (archivedExecutionGraph != null) {
			return WebMonitorUtils.createDetailsForJob(archivedExecutionGraph);
		} else {
			return null;
		}
	}

	@Override
	public void close() throws IOException {
		serializableExecutionGraphs.clear();
	}
}
