package org.apache.flink.runtime.resourcemanager.registration;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.jobmaster.JobMasterGateway;
import org.apache.flink.runtime.jobmaster.JobMasterId;
import org.apache.flink.util.Preconditions;

/**
 * Container for JobManager related registration information, such as the leader id or the job id.
 */
@Getter
public class JobManagerRegistration {
	private final JobID jobID;

	private final ResourceID jobManagerResourceID;

	private final JobMasterGateway jobManagerGateway;

	public JobManagerRegistration(
			JobID jobID,
			ResourceID jobManagerResourceID,
			JobMasterGateway jobManagerGateway) {
		this.jobID = Preconditions.checkNotNull(jobID);
		this.jobManagerResourceID = Preconditions.checkNotNull(jobManagerResourceID);
		this.jobManagerGateway = Preconditions.checkNotNull(jobManagerGateway);
	}


	public JobMasterId getJobMasterId() {
		return jobManagerGateway.getFencingToken();
	}
}
