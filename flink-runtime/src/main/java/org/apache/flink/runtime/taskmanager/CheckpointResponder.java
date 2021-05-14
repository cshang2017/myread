package org.apache.flink.runtime.taskmanager;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.checkpoint.CheckpointMetrics;
import org.apache.flink.runtime.checkpoint.TaskStateSnapshot;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;

/**
 * Responder for checkpoint acknowledge and decline messages in the {@link Task}.
 */
public interface CheckpointResponder {

	/**
	 * Acknowledges the given checkpoint.
	 *
	 * @param jobID
	 *             Job ID of the running job
	 * @param executionAttemptID
	 *             Execution attempt ID of the running task
	 * @param checkpointId
	 *             Meta data for this checkpoint
	 * @param checkpointMetrics
	 *             Metrics of this checkpoint
	 * @param subtaskState
	 *             State handles for the checkpoint
	 */
	void acknowledgeCheckpoint(
		JobID jobID,
		ExecutionAttemptID executionAttemptID,
		long checkpointId,
		CheckpointMetrics checkpointMetrics,
		TaskStateSnapshot subtaskState);

	/**
	 * Declines the given checkpoint.
	 *
	 * @param jobID Job ID of the running job
	 * @param executionAttemptID Execution attempt ID of the running task
	 * @param checkpointId The ID of the declined checkpoint
	 * @param cause The optional cause why the checkpoint was declined   
	 */
	void declineCheckpoint(
		JobID jobID,
		ExecutionAttemptID executionAttemptID,
		long checkpointId,
		Throwable cause);
}
