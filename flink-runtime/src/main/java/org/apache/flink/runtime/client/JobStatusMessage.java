package org.apache.flink.runtime.client;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;

/**
 * A simple message that holds the state of a job execution.
 */
@Getter
@AllArgsConstructor
public class JobStatusMessage implements java.io.Serializable {

	private final JobID jobId;

	private final String jobName;

	private final JobStatus jobState;

	private final long startTime;


}
