package org.apache.flink.runtime.taskmanager;

import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.taskexecutor.TaskExecutor;

/**
 * Interface for the communication of the {@link Task} with the {@link TaskExecutor}.
 */
public interface TaskManagerActions {

	/**
	 * Notifies the task manager about a fatal error occurred in the task.
	 *
	 * @param message Message to report
	 * @param cause Cause of the fatal error
	 */
	void notifyFatalError(String message, Throwable cause);

	/**
	 * Tells the task manager to fail the given task.
	 *
	 * @param executionAttemptID Execution attempt ID of the task to fail
	 * @param cause Cause of the failure
	 */
	void failTask(ExecutionAttemptID executionAttemptID, Throwable cause);

	/**
	 * Notifies the task manager about the task execution state update.
	 *
	 * @param taskExecutionState Task execution state update
	 */
	void updateTaskExecutionState(TaskExecutionState taskExecutionState);
}
