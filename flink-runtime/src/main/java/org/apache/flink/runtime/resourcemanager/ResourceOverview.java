

package org.apache.flink.runtime.resourcemanager;

import org.apache.flink.runtime.clusterframework.types.ResourceProfile;

import java.io.Serializable;

/**
 * Class containing information about the available cluster resources.
 */
public class ResourceOverview implements Serializable {

	private static final ResourceOverview EMPTY_RESOURCE_OVERVIEW = new ResourceOverview(0, 0, 0, ResourceProfile.ZERO, ResourceProfile.ZERO);

	private final int numberTaskManagers;

	private final int numberRegisteredSlots;

	private final int numberFreeSlots;

	private final ResourceProfile totalResource;

	private final ResourceProfile freeResource;

	public ResourceOverview(int numberTaskManagers, int numberRegisteredSlots, int numberFreeSlots, ResourceProfile totalResource, ResourceProfile freeResource) {
		this.numberTaskManagers = numberTaskManagers;
		this.numberRegisteredSlots = numberRegisteredSlots;
		this.numberFreeSlots = numberFreeSlots;
		this.totalResource = totalResource;
		this.freeResource = freeResource;
	}

	public int getNumberTaskManagers() {
		return numberTaskManagers;
	}

	public int getNumberRegisteredSlots() {
		return numberRegisteredSlots;
	}

	public int getNumberFreeSlots() {
		return numberFreeSlots;
	}

	public ResourceProfile getTotalResource() {
		return totalResource;
	}

	public ResourceProfile getFreeResource() {
		return freeResource;
	}

	public static ResourceOverview empty() {
		return EMPTY_RESOURCE_OVERVIEW;
	}
}
