package org.apache.flink.runtime.jobmaster.slotpool;

import org.apache.flink.runtime.jobmaster.SlotContext;

/**
 * The context of an {@link AllocatedSlot}. This represent an interface to classes outside the slot pool to interact
 * with allocated slots.
 */
public interface PhysicalSlot extends SlotContext {

	/**
	 * Tries to assign the given payload to this allocated slot. This only works if there has not
	 * been another payload assigned to this slot.
	 *
	 * @param payload to assign to this slot
	 * @return true if the payload could be assigned, otherwise false
	 */
	boolean tryAssignPayload(Payload payload);

	/**
	 * Payload which can be assigned to an {@link AllocatedSlot}.
	 */
	interface Payload {

		/**
		 * Releases the payload
		 *
		 * @param cause of the payload release
		 */
		void release(Throwable cause);
	}
}
