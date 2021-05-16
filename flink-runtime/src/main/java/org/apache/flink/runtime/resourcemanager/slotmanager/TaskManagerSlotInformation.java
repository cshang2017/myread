package org.apache.flink.runtime.resourcemanager.slotmanager;

import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.clusterframework.types.SlotID;
import org.apache.flink.runtime.instance.InstanceID;

/**
 * Basic information about a {@link TaskManagerSlot}.
 */
public interface TaskManagerSlotInformation {

	SlotID getSlotId();

	InstanceID getInstanceId();

	/**
	 * Returns true if the required {@link ResourceProfile} can be fulfilled
	 * by this slot.
	 *
	 * @param required resources
	 * @return true if the this slot can fulfill the resource requirements
	 */
	boolean isMatchingRequirement(ResourceProfile required);

	/**
	 * Get resource profile of this slot.
	 *
	 * @return resource profile of this slot
	 */
	ResourceProfile getResourceProfile();
}
