package org.apache.flink.runtime.taskexecutor.slot;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.clusterframework.types.AllocationID;

/**
 * Exception indicating that the given {@link TaskSlot} was not in state active.
 */
public class SlotNotActiveException extends Exception {


	public SlotNotActiveException(JobID jobId, AllocationID allocationId) {
		super("No active slot for job " + jobId + " with allocation id " + allocationId + '.');
	}
}
