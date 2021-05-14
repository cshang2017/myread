package org.apache.flink.api.common;

import org.apache.flink.annotation.Public;

/**
 * The result of submitting a job to a JobManager.
 */
@Public
public class JobSubmissionResult {

	private final JobID jobID;

	public JobSubmissionResult(JobID jobID) {
		this.jobID = jobID;
	}

	/**
	 * Returns the JobID assigned to the job by the Flink runtime.
	 *
	 * @return jobID, or null if the job has been executed on a runtime without JobIDs or if the execution failed.
	 */
	public JobID getJobID() {
		return jobID;
	}

	/**
	 * Checks if this JobSubmissionResult is also a JobExecutionResult.
	 * See {@code getJobExecutionResult} to retrieve the JobExecutionResult.
	 * @return True if this is a JobExecutionResult, false otherwise
	 */
	public boolean isJobExecutionResult() {
		return false;
	}

	/**
	 * Returns the JobExecutionResult if available.
	 * @return The JobExecutionResult
	 * @throws ClassCastException if this is not a JobExecutionResult
	 */
	public JobExecutionResult getJobExecutionResult() {
		throw new ClassCastException("This JobSubmissionResult is not a JobExecutionResult.");
	}
}
