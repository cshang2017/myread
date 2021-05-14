package org.apache.flink.runtime.jobmanager;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.jobgraph.JobGraph;

/**
 * {@link JobGraphWriter} implementation which does not allow to store
 * {@link JobGraph}.
 */
public enum ThrowingJobGraphWriter implements JobGraphWriter {
	INSTANCE;

	@Override
	public void putJobGraph(JobGraph jobGraph) {
		throw new UnsupportedOperationException("Cannot store job graphs.");
	}

	@Override
	public void removeJobGraph(JobID jobId) {}

	@Override
	public void releaseJobGraph(JobID jobId) {}
}
