package org.apache.flink.runtime.executiongraph;

import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;

import javax.annotation.Nullable;

/**
 * Common interface for the runtime {@link ExecutionVertex} and {@link ArchivedExecutionVertex}.
 */
public interface AccessExecutionVertex {
	/**
	 * Returns the name of this execution vertex in the format "myTask (2/7)".
	 */
	String getTaskNameWithSubtaskIndex();

	/**
	 * Returns the subtask index of this execution vertex.
	 */
	int getParallelSubtaskIndex();

	/**
	 * Returns the current execution for this execution vertex.
	 */
	AccessExecution getCurrentExecutionAttempt();

	/**
	 * Returns the current {@link ExecutionState} for this execution vertex.
	 */
	ExecutionState getExecutionState();

	/**
	 * Returns the timestamp for the given {@link ExecutionState}.
	 */
	long getStateTimestamp(ExecutionState state);

	/**
	 * Returns the exception that caused the job to fail. This is the first root exception
	 * that was not recoverable and triggered job failure.
	 */
	String getFailureCauseAsString();

	/**
	 * Returns the {@link TaskManagerLocation} for this execution vertex.
	 */
	TaskManagerLocation getCurrentAssignedResourceLocation();

	/**
	 * Returns the execution for the given attempt number.
	 */
	@Nullable
	AccessExecution getPriorExecutionAttempt(int attemptNumber);
}
