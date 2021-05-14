package org.apache.flink.runtime.jobmaster;

import org.apache.flink.runtime.clusterframework.types.AllocationID;

import java.io.Serializable;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Information about an allocated slot which is owned by a JobMaster.
 */
public class AllocatedSlotInfo implements Serializable {

	private final int slotIndex;

	private final AllocationID allocationId;

	public AllocatedSlotInfo(int index, AllocationID allocationId) {
		checkArgument(index >= 0);
		this.slotIndex = index;
		this.allocationId = checkNotNull(allocationId);
	}

	public AllocationID getAllocationId() {
		return allocationId;
	}

	public int getSlotIndex() {
		return slotIndex;
	}


}
