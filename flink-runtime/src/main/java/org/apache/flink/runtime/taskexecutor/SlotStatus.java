package org.apache.flink.runtime.taskexecutor;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.clusterframework.types.SlotID;

import java.io.Serializable;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * This describes the slot current status which located in TaskManager.
 */
public class SlotStatus implements Serializable {

	/** SlotID to identify a slot. */
	private final SlotID slotID;

	/** The resource profile of the slot. */
	private final ResourceProfile resourceProfile;

	/** If the slot is allocated, allocationId identify its allocation; else, allocationId is null. */
	private final AllocationID allocationID;

	/** If the slot is allocated, jobId identify which job this slot is allocated to; else, jobId is null. */
	private final JobID jobID;

	public SlotStatus(SlotID slotID, ResourceProfile resourceProfile) {
		this(slotID, resourceProfile, null, null);
	}

	public SlotStatus(
		SlotID slotID,
		ResourceProfile resourceProfile,
		JobID jobID,
		AllocationID allocationID) {
		this.slotID = checkNotNull(slotID, "slotID cannot be null");
		this.resourceProfile = checkNotNull(resourceProfile, "profile cannot be null");
		this.allocationID = allocationID;
		this.jobID = jobID;
	}

	/**
	 * Get the unique identification of this slot.
	 *
	 * @return The slot id
	 */
	public SlotID getSlotID() {
		return slotID;
	}

	/**
	 * Get the resource profile of this slot.
	 *
	 * @return The resource profile
	 */
	public ResourceProfile getResourceProfile() {
		return resourceProfile;
	}

	/**
	 * Get the allocation id of this slot.
	 *
	 * @return The allocation id if this slot is allocated, otherwise null
	 */
	public AllocationID getAllocationID() {
		return allocationID;
	}

	/**
	 * Get the job id of the slot allocated for.
	 *
	 * @return The job id if this slot is allocated, otherwise null
	 */
	public JobID getJobID() {
		return jobID;
	}

	
}
