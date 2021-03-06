
package org.apache.flink.runtime.resourcemanager;

import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.instance.HardwareDescription;

import java.io.Serializable;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Information provided by the TaskExecutor when it registers to the ResourceManager.
 */
public class TaskExecutorRegistration implements Serializable {

	/**
	 * The address of the TaskExecutor that registers.
	 */
	private final String taskExecutorAddress;

	/**
	 * The resource ID of the TaskExecutor that registers.
	 */
	private final ResourceID resourceId;

	/**
	 * Port used for data communication between TaskExecutors.
	 */
	private final int dataPort;

	/**
	 * HardwareDescription of the registering TaskExecutor.
	 */
	private final HardwareDescription hardwareDescription;

	/**
	 * The default resource profile for slots requested with unknown resource requirements.
	 */
	private final ResourceProfile defaultSlotResourceProfile;

	/**
	 * The task executor total resource profile.
	 */
	private final ResourceProfile totalResourceProfile;

	public TaskExecutorRegistration(
			final String taskExecutorAddress,
			final ResourceID resourceId,
			final int dataPort,
			final HardwareDescription hardwareDescription,
			final ResourceProfile defaultSlotResourceProfile,
			final ResourceProfile totalResourceProfile) {
		this.taskExecutorAddress = checkNotNull(taskExecutorAddress);
		this.resourceId = checkNotNull(resourceId);
		this.dataPort = dataPort;
		this.hardwareDescription = checkNotNull(hardwareDescription);
		this.defaultSlotResourceProfile = checkNotNull(defaultSlotResourceProfile);
		this.totalResourceProfile = checkNotNull(totalResourceProfile);
	}

	public String getTaskExecutorAddress() {
		return taskExecutorAddress;
	}

	public ResourceID getResourceId() {
		return resourceId;
	}

	public int getDataPort() {
		return dataPort;
	}

	public HardwareDescription getHardwareDescription() {
		return hardwareDescription;
	}

	public ResourceProfile getDefaultSlotResourceProfile() {
		return defaultSlotResourceProfile;
	}

	public ResourceProfile getTotalResourceProfile() {
		return totalResourceProfile;
	}
}
