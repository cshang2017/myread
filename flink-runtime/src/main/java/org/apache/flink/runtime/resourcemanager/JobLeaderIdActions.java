

package org.apache.flink.runtime.resourcemanager;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.jobmaster.JobMasterId;

import java.util.UUID;

/**
 * Interface for actions called by the {@link JobLeaderIdService}.
 */
public interface JobLeaderIdActions {

	/**
	 * Callback when a monitored job leader lost its leadership.
	 *
	 * @param jobId identifying the job whose leader lost leadership
	 * @param oldJobMasterId of the job manager which lost leadership
	 */
	void jobLeaderLostLeadership(JobID jobId, JobMasterId oldJobMasterId);

	/**
	 * Notify a job timeout. The job is identified by the given JobID. In order to check
	 * for the validity of the timeout the timeout id of the triggered timeout is provided.
	 *
	 * @param jobId JobID which identifies the timed out job
	 * @param timeoutId Id of the calling timeout to differentiate valid from invalid timeouts
	 */
	void notifyJobTimeout(JobID jobId, UUID timeoutId);

	/**
	 * Callback to report occurring errors.
	 *
	 * @param error which has occurred
	 */
	void handleError(Throwable error);
}
