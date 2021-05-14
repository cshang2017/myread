package org.apache.flink.runtime.jobmaster;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.JobException;

/**
 * Exception indicating that a Flink job has not been finished.
 */
public class JobNotFinishedException extends JobException {

	public JobNotFinishedException(JobID jobId) {
		super("The job (" + jobId + ") has not been finished.");
	}
}
