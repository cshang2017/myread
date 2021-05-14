package org.apache.flink.runtime.taskexecutor.slot;

import org.apache.flink.runtime.clusterframework.types.AllocationID;

/**
 * Exception indicating that a {@link TaskSlot} could not be found.
 */
public class SlotNotFoundException extends Exception {


	public SlotNotFoundException(AllocationID allocationId) {
		this("Could not find slot for " + allocationId + '.');
	}

	public SlotNotFoundException(String message) {
		super(message);
	}
}
