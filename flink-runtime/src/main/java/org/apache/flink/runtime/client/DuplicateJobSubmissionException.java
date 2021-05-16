
package org.apache.flink.runtime.client;

import org.apache.flink.api.common.JobID;

/**
 * Exception which is returned upon job submission if the submitted job
 * is currently being executed.
 */
public class DuplicateJobSubmissionException extends JobSubmissionException {

	public DuplicateJobSubmissionException(JobID jobID) {
		super(jobID, "Job has already been submitted.");
	}
}
