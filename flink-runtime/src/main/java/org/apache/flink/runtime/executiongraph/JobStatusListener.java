package org.apache.flink.runtime.executiongraph;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;

/**
 * Interface for observers that monitor the status of a job.
 */
public interface JobStatusListener {

	/**
	 * This method is called whenever the status of the job changes.
	 * 
	 * @param jobId         The ID of the job.
	 * @param newJobStatus  The status the job switched to.
	 * @param timestamp     The timestamp when the status transition occurred.
	 * @param error         In case the job status switches to a failure state, this is the
	 *                      exception that caused the failure.
	 */
	void jobStatusChanges(JobID jobId, JobStatus newJobStatus, long timestamp, Throwable error);
}
