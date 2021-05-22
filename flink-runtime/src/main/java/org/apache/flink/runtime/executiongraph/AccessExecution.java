package org.apache.flink.runtime.executiongraph;

import org.apache.flink.runtime.accumulators.StringifiedAccumulatorResult;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;

/**
 * Common interface for the runtime {@link Execution and {@link ArchivedExecution}.
 */
public interface AccessExecution {
	/**
	 * Returns the {@link ExecutionAttemptID} for this Execution.
	 */
	ExecutionAttemptID getAttemptId();

	/**
	 * Returns the attempt number for this execution.
	 */
	int getAttemptNumber();

	/**
	 * Returns the timestamps for every {@link ExecutionState}.
	 */
	long[] getStateTimestamps();

	/**
	 * Returns the current {@link ExecutionState} for this execution.
	 */
	ExecutionState getState();

	/**
	 * Returns the {@link TaskManagerLocation} for this execution.
	 */
	TaskManagerLocation getAssignedResourceLocation();

	/**
	 * Returns the exception that caused the job to fail. This is the first root exception
	 * that was not recoverable and triggered job failure.
	 */
	String getFailureCauseAsString();

	/**
	 * Returns the timestamp for the given {@link ExecutionState}.
	 *
	 * @param state state for which the timestamp should be returned
	 * @return timestamp for the given state
	 */
	long getStateTimestamp(ExecutionState state);

	/**
	 * Returns the user-defined accumulators as strings.
	 */
	StringifiedAccumulatorResult[] getUserAccumulatorsStringified();

	/**
	 * Returns the subtask index of this execution.
	 */
	int getParallelSubtaskIndex();

	IOMetrics getIOMetrics();
}
