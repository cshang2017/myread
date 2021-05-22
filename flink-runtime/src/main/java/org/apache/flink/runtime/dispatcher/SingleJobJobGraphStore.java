package org.apache.flink.runtime.dispatcher;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobmanager.JobGraphStore;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.Preconditions;

import java.util.Collection;
import java.util.Collections;
import java.util.Objects;

/**
 * {@link JobGraphStore} implementation for a single job.
 */
public class SingleJobJobGraphStore implements JobGraphStore {

	private final JobGraph jobGraph;

	public SingleJobJobGraphStore(JobGraph jobGraph) {
		this.jobGraph = Preconditions.checkNotNull(jobGraph);
	}

	@Override
	public void start(JobGraphListener jobGraphListener) throws Exception {
		// noop
	}

	@Override
	public void stop() throws Exception {
		// noop
	}

	@Override
	public JobGraph recoverJobGraph(JobID jobId) throws Exception {
		if (jobGraph.getJobID().equals(jobId)) {
			return jobGraph;
		} else {
			throw new FlinkException("Could not recover job graph " + jobId + '.');
		}
	}

	@Override
	public void putJobGraph(JobGraph jobGraph) throws Exception {
		if (!Objects.equals(this.jobGraph.getJobID(), jobGraph.getJobID())) {
			throw new FlinkException("Cannot put additional jobs into this submitted job graph store.");
		}
	}

	@Override
	public void removeJobGraph(JobID jobId) {
		// ignore
	}

	@Override
	public void releaseJobGraph(JobID jobId) {
		// ignore
	}

	@Override
	public Collection<JobID> getJobIds() {
		return Collections.singleton(jobGraph.getJobID());
	}
}
