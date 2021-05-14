package org.apache.flink.runtime.jobmaster;

import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;

/**
 * Interface that provides basic information in the context of a slot.
 */
public interface SlotInfo {

	/**
	 * Gets the id under which the slot has been allocated on the TaskManager. This id uniquely identifies the
	 * physical slot.
	 *
	 * @return The id under which the slot has been allocated on the TaskManager
	 */
	AllocationID getAllocationId();

	/**
	 * Gets the location info of the TaskManager that offers this slot.
	 *
	 * @return The location info of the TaskManager that offers this slot
	 */
	TaskManagerLocation getTaskManagerLocation();

	/**
	 * Gets the number of the slot.
	 *
	 * @return The number of the slot on the TaskManager.
	 */
	int getPhysicalSlotNumber();

	/**
	 * Returns the resource profile of the slot.
	 *
	 * @return the resource profile of the slot.
	 */
	ResourceProfile getResourceProfile();
}
