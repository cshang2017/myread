package org.apache.flink.runtime.client;

import org.apache.flink.api.common.JobID;

/**
 * This exception denotes an error while submitting a job to the JobManager.
 */
public class JobSubmissionException extends JobExecutionException {

	public JobSubmissionException(final JobID jobID, final String msg, final Throwable cause) {
		super(jobID, msg, cause);
	}

	public JobSubmissionException(final JobID jobID, final String msg) {
		super(jobID, msg);
	}
}
