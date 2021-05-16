

package org.apache.flink.runtime.client;

import org.apache.flink.api.common.JobID;

/**
 * An exception which is thrown by the JobClient if a job is aborted as a result of a user
 * cancellation.
 */
public class JobCancellationException extends JobExecutionException {

	public JobCancellationException(JobID jobID, String msg, Throwable cause){
		super(jobID, msg, cause);
	}
}
