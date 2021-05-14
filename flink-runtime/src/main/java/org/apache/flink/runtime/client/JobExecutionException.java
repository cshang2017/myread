package org.apache.flink.runtime.client;

import org.apache.flink.api.common.JobID;
import org.apache.flink.util.FlinkException;

/**
 * This exception is the base exception for all exceptions that denote any failure during
 * the execution of a job.
 */
public class JobExecutionException extends FlinkException {

	private final JobID jobID;

	/**
	 * Constructs a new job execution exception.
	 *
	 * @param jobID The job's ID.
	 * @param msg The cause for the execution exception.
	 * @param cause The cause of the exception
	 */
	public JobExecutionException(JobID jobID, String msg, Throwable cause) {
		super(msg, cause);
		this.jobID = jobID;
	}

	public JobExecutionException(JobID jobID, String msg) {
		super(msg);
		this.jobID = jobID;
	}

	public JobExecutionException(JobID jobID, Throwable cause) {
		super(cause);
		this.jobID = jobID;
	}

	public JobID getJobID() {
		return jobID;
	}
}
