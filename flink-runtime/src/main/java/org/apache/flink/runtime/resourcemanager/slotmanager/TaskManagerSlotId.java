package org.apache.flink.runtime.resourcemanager.slotmanager;

import org.apache.flink.util.AbstractID;

/**
 * Id of {@link TaskManagerSlot} and {@link PendingTaskManagerSlot}.
 */
public class TaskManagerSlotId extends AbstractID {

	private TaskManagerSlotId() {}

	public static TaskManagerSlotId generate() {
		return new TaskManagerSlotId();
	}
}
