package org.apache.flink.runtime.taskexecutor.slot;

/**
 * Internal task slot state
 */
enum TaskSlotState {
	ACTIVE, // Slot is in active use by a job manager responsible for a job
	ALLOCATED, // Slot has been allocated for a job but not yet given to a job manager
	RELEASING // Slot is not empty but tasks are failed. Upon removal of all tasks, it will be released
}
