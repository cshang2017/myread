package org.apache.flink.runtime.jobmaster.slotpool;

import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.jobmaster.SlotInfo;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;

/**
 * Container for {@link SlotInfo} and the task executors utilization (freeSlots / totalOfferedSlots).
 */
public final class SlotInfoWithUtilization implements SlotInfo {
	private final SlotInfo slotInfoDelegate;
	private final double taskExecutorUtilization;

	private SlotInfoWithUtilization(SlotInfo slotInfo, double taskExecutorUtilization) {
		this.slotInfoDelegate = slotInfo;
		this.taskExecutorUtilization = taskExecutorUtilization;
	}

	double getTaskExecutorUtilization() {
		return taskExecutorUtilization;
	}

	@Override
	public AllocationID getAllocationId() {
		return slotInfoDelegate.getAllocationId();
	}

	@Override
	public TaskManagerLocation getTaskManagerLocation() {
		return slotInfoDelegate.getTaskManagerLocation();
	}

	@Override
	public int getPhysicalSlotNumber() {
		return slotInfoDelegate.getPhysicalSlotNumber();
	}

	@Override
	public ResourceProfile getResourceProfile() {
		return slotInfoDelegate.getResourceProfile();
	}

	public static SlotInfoWithUtilization from(SlotInfo slotInfo, double taskExecutorUtilization) {
		return new SlotInfoWithUtilization(slotInfo, taskExecutorUtilization);
	}
}
