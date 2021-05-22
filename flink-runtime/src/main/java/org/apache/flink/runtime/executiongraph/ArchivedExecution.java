package org.apache.flink.runtime.executiongraph;

import org.apache.flink.runtime.accumulators.StringifiedAccumulatorResult;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.util.ExceptionUtils;

import java.io.Serializable;

public class ArchivedExecution implements AccessExecution, Serializable {
	// --------------------------------------------------------------------------------------------

	private final ExecutionAttemptID attemptId;

	private final long[] stateTimestamps;

	private final int attemptNumber;

	private final ExecutionState state;

	private final String failureCause;          // once assigned, never changes

	private final TaskManagerLocation assignedResourceLocation; // for the archived execution

	private final AllocationID assignedAllocationID;

	/* Continuously updated map of user-defined accumulators */
	private final StringifiedAccumulatorResult[] userAccumulators;

	private final int parallelSubtaskIndex;

	private final IOMetrics ioMetrics;

	public ArchivedExecution(Execution execution) {
		this(
			execution.getUserAccumulatorsStringified(),
			execution.getIOMetrics(),
			execution.getAttemptId(),
			execution.getAttemptNumber(),
			execution.getState(),
			ExceptionUtils.stringifyException(execution.getFailureCause()),
			execution.getAssignedResourceLocation(),
			execution.getAssignedAllocationID(),
			execution.getVertex().getParallelSubtaskIndex(),
			execution.getStateTimestamps());
	}

	public ArchivedExecution(
			StringifiedAccumulatorResult[] userAccumulators, IOMetrics ioMetrics,
			ExecutionAttemptID attemptId, int attemptNumber, ExecutionState state, String failureCause,
			TaskManagerLocation assignedResourceLocation, AllocationID assignedAllocationID,  int parallelSubtaskIndex,
			long[] stateTimestamps) {
		this.userAccumulators = userAccumulators;
		this.ioMetrics = ioMetrics;
		this.failureCause = failureCause;
		this.assignedResourceLocation = assignedResourceLocation;
		this.attemptNumber = attemptNumber;
		this.attemptId = attemptId;
		this.state = state;
		this.stateTimestamps = stateTimestamps;
		this.parallelSubtaskIndex = parallelSubtaskIndex;
		this.assignedAllocationID = assignedAllocationID;
	}

	// --------------------------------------------------------------------------------------------
	//   Accessors
	// --------------------------------------------------------------------------------------------

	@Override
	public ExecutionAttemptID getAttemptId() {
		return attemptId;
	}

	@Override
	public int getAttemptNumber() {
		return attemptNumber;
	}

	@Override
	public long[] getStateTimestamps() {
		return stateTimestamps;
	}

	@Override
	public ExecutionState getState() {
		return state;
	}

	@Override
	public TaskManagerLocation getAssignedResourceLocation() {
		return assignedResourceLocation;
	}

	public AllocationID getAssignedAllocationID() {
		return assignedAllocationID;
	}

	@Override
	public String getFailureCauseAsString() {
		return failureCause;
	}

	@Override
	public long getStateTimestamp(ExecutionState state) {
		return this.stateTimestamps[state.ordinal()];
	}

	@Override
	public StringifiedAccumulatorResult[] getUserAccumulatorsStringified() {
		return userAccumulators;
	}


	@Override
	public int getParallelSubtaskIndex() {
		return parallelSubtaskIndex;
	}

	@Override
	public IOMetrics getIOMetrics() {
		return ioMetrics;
	}
}
