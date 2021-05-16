

package org.apache.flink.runtime.resourcemanager;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;

import java.io.Serializable;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * This describes the requirement of the slot, mainly used by JobManager requesting slot from ResourceManager.
 */
public class SlotRequest implements Serializable {

	/** The JobID of the slot requested for */
	private final JobID jobId;

	/** The unique identification of this request */
	private final AllocationID allocationId;

	/** The resource profile of the required slot */
	private final ResourceProfile resourceProfile;

	/** Address of the emitting job manager */
	private final String targetAddress;

	public SlotRequest(
			JobID jobId,
			AllocationID allocationId,
			ResourceProfile resourceProfile,
			String targetAddress) {
		this.jobId = checkNotNull(jobId);
		this.allocationId = checkNotNull(allocationId);
		this.resourceProfile = checkNotNull(resourceProfile);
		this.targetAddress = checkNotNull(targetAddress);
	}

	/**
	 * Get the JobID of the slot requested for.
	 * @return The job id
	 */
	public JobID getJobId() {
		return jobId;
	}

	/**
	 * Get the unique identification of this request
	 * @return the allocation id
	 */
	public AllocationID getAllocationId() {
		return allocationId;
	}

	/**
	 * Get the resource profile of the desired slot
	 * @return The resource profile
	 */
	public ResourceProfile getResourceProfile() {
		return resourceProfile;
	}

	public String getTargetAddress() {
		return targetAddress;
	}
}
