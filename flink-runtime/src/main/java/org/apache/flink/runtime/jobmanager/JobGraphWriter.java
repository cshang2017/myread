package org.apache.flink.runtime.jobmanager;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.jobgraph.JobGraph;

/**
 * Allows to store and remove job graphs.
 */
public interface JobGraphWriter {
	/**
	 * Adds the {@link JobGraph} instance.
	 *
	 * <p>If a job graph with the same {@link JobID} exists, it is replaced.
	 */
	void putJobGraph(JobGraph jobGraph) throws Exception;

	/**
	 * Removes the {@link JobGraph} with the given {@link JobID} if it exists.
	 */
	void removeJobGraph(JobID jobId) throws Exception;

	/**
	 * Releases the locks on the specified {@link JobGraph}.
	 *
	 * Releasing the locks allows that another instance can delete the job from
	 * the {@link JobGraphStore}.
	 *
	 * @param jobId specifying the job to release the locks for
	 * @throws Exception if the locks cannot be released
	 */
	void releaseJobGraph(JobID jobId) throws Exception;
}
