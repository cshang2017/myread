package org.apache.flink.runtime.taskexecutor;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.jobmaster.JMTMRegistrationSuccess;
import org.apache.flink.runtime.jobmaster.JobMasterGateway;
import org.apache.flink.runtime.jobmaster.JobMasterId;

/**
 * Listener for the {@link JobLeaderService}. The listener is notified whenever a job manager
 * gained leadership for a registered job and the service could establish a connection to it.
 * Furthermore, the listener is notified when a job manager loses leadership for a job. In case
 * of an error, the {@link #handleError(Throwable)}} is called.
 */
public interface JobLeaderListener {

	/**
	 * Callback if a job manager has gained leadership for the job identified by the job id and a
	 * connection could be established to this job manager.
	 *
	 * @param jobId identifying the job for which the job manager has gained leadership
	 * @param jobManagerGateway to the job leader
	 * @param registrationMessage containing further registration information
	 */
	void jobManagerGainedLeadership(JobID jobId, JobMasterGateway jobManagerGateway, JMTMRegistrationSuccess registrationMessage);

	/**
	 * Callback if the job leader for the job with the given job id lost its leadership.
	 *
	 * @param jobId identifying the job whose leader has lost leadership
	 * @param jobMasterId old JobMasterId
	 */
	void jobManagerLostLeadership(JobID jobId, JobMasterId jobMasterId);

	/**
	 * Callback for errors which might occur in the {@link JobLeaderService}.
	 *
	 * @param throwable cause
	 */
	void handleError(Throwable throwable);
}
