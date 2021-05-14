
package org.apache.flink.runtime.taskexecutor.slot;

import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.util.Preconditions;

import java.io.Serializable;

/**
 * Describe the slot offering to job manager provided by task manager.
 */
public class SlotOffer implements Serializable {


	/** Allocation id of this slot, this would be the only identifier for this slot offer */
	private AllocationID allocationId;

	/** Index of the offered slot */
	private final int slotIndex;

	/** The resource profile of the offered slot */
	private final ResourceProfile resourceProfile;

	public SlotOffer(final AllocationID allocationID, final int index, final ResourceProfile resourceProfile) {
		this.allocationId = Preconditions.checkNotNull(allocationID);
		this.slotIndex = index;
		this.resourceProfile = Preconditions.checkNotNull(resourceProfile);
	}

	public AllocationID getAllocationId() {
		return allocationId;
	}

	public int getSlotIndex() {
		return slotIndex;
	}

	public ResourceProfile getResourceProfile() {
		return resourceProfile;
	}

}
