package org.apache.flink.runtime.jobmaster;

import org.apache.flink.runtime.jobmanager.slots.TaskManagerGateway;

/**
 * Interface for the context of a {@link LogicalSlot}. This context contains information
 * about the underlying allocated slot and how to communicate with the TaskManager on which
 * it was allocated.
 */
public interface SlotContext extends SlotInfo {

	/**
	 * Gets the actor gateway that can be used to send messages to the TaskManager.
	 * <p>
	 * This method should be removed once the new interface-based RPC abstraction is in place
	 *
	 * @return The gateway that can be used to send messages to the TaskManager.
	 */
	TaskManagerGateway getTaskManagerGateway();
}
