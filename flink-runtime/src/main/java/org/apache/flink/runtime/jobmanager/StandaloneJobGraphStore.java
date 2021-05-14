package org.apache.flink.runtime.jobmanager;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.jobgraph.JobGraph;

import java.util.Collection;
import java.util.Collections;

/**
 * {@link JobGraph} instances for JobManagers running in {@link HighAvailabilityMode#NONE}.
 *
 * <p>All operations are NoOps, because {@link JobGraph} instances cannot be recovered in this
 * recovery mode.
 */
public class StandaloneJobGraphStore implements JobGraphStore {

	@Override
	public void start(JobGraphListener jobGraphListener) throws Exception {
		// Nothing to do
	}

	@Override
	public void stop() {
		// Nothing to do
	}

	@Override
	public void putJobGraph(JobGraph jobGraph) {
		// Nothing to do
	}

	@Override
	public void removeJobGraph(JobID jobId) {
		// Nothing to do
	}

	@Override
	public void releaseJobGraph(JobID jobId) {
		// nothing to do
	}

	@Override
	public Collection<JobID> getJobIds() {
		return Collections.emptyList();
	}

	@Override
	public JobGraph recoverJobGraph(JobID jobId) {
		return null;
	}
}
