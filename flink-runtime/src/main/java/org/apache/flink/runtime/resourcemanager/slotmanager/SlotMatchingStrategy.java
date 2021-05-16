package org.apache.flink.runtime.resourcemanager.slotmanager;

import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.instance.InstanceID;

import java.util.Collection;
import java.util.Optional;
import java.util.function.Function;

/**
 * Strategy how to find a matching slot.
 */
public interface SlotMatchingStrategy {

	/**
	 * Finds a matching slot for the requested {@link ResourceProfile} given the
	 * collection of free slots and the total number of slots per TaskExecutor.
	 *
	 * @param requestedProfile to find a matching slot for
	 * @param freeSlots collection of free slots
	 * @param numberRegisteredSlotsLookup lookup for the number of registered slots
	 * @return Returns a matching slots or {@link Optional#empty()} if there is none
	 */
	<T extends TaskManagerSlotInformation> Optional<T> findMatchingSlot(
		ResourceProfile requestedProfile,
		Collection<T> freeSlots,
		Function<InstanceID, Integer> numberRegisteredSlotsLookup);
}
