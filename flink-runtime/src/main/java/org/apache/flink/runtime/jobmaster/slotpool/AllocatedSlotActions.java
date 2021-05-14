package org.apache.flink.runtime.jobmaster.slotpool;

import org.apache.flink.runtime.jobmaster.SlotRequestId;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Interface for components which have to perform actions on allocated slots.
 */
public interface AllocatedSlotActions {

	/**
	 * Releases the slot with the given {@link SlotRequestId}. Additionally, one can provide a cause for the slot release.
	 *
	 * @param slotRequestId identifying the slot to release
	 * @param cause of the slot release, null if none
	 */
	void releaseSlot(
		@Nonnull SlotRequestId slotRequestId,
		@Nullable Throwable cause);
}
