package org.apache.flink.runtime.taskexecutor.slot;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;

import java.util.concurrent.CompletableFuture;

/**
 * Payload interface for {@link org.apache.flink.runtime.taskexecutor.slot.TaskSlot}.
 */
public interface TaskSlotPayload {
	JobID getJobID();

	ExecutionAttemptID getExecutionId();

	AllocationID getAllocationId();

	CompletableFuture<?> getTerminationFuture();

	/**
	 * Fail the payload with the given throwable. This operation should
	 * eventually complete the termination future.
	 *
	 * @param cause of the failure
	 */
	void failExternally(Throwable cause);
}
