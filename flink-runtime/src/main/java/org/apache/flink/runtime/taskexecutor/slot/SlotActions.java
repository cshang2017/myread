package org.apache.flink.runtime.taskexecutor.slot;

import org.apache.flink.runtime.clusterframework.types.AllocationID;

import java.util.UUID;

/**
 * Interface to trigger slot actions from within the {@link TaskSlotTable}.
 */
public interface SlotActions {

	/**
	 * Free the task slot with the given allocation id.
	 *
	 * @param allocationId to identify the slot to be freed
	 */
	void freeSlot(AllocationID allocationId);

	/**
	 * Timeout the task slot for the given allocation id. The timeout is identified by the given
	 * ticket to filter invalid timeouts out.
	 *
	 * @param allocationId identifying the task slot to be timed out
	 * @param ticket allowing to filter invalid timeouts out
	 */
	void timeoutSlot(AllocationID allocationId, UUID ticket);
}
