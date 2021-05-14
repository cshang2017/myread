package org.apache.flink.runtime.client;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;

/**
 * A simple message that holds the state of a job execution.
 */
public class JobStatusMessage implements java.io.Serializable {

	private final JobID jobId;

	private final String jobName;

	private final JobStatus jobState;

	private final long startTime;

	public JobStatusMessage(JobID jobId, String jobName, JobStatus jobState, long startTime) {
		this.jobId = jobId;
		this.jobName = jobName;
		this.jobState = jobState;
		this.startTime = startTime;
	}

	public JobID getJobId() {
		return jobId;
	}

	public String getJobName() {
		return jobName;
	}

	public JobStatus getJobState() {
		return jobState;
	}

	public long getStartTime() {
		return startTime;
	}
}
