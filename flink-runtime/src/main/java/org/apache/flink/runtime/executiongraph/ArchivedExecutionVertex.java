package org.apache.flink.runtime.executiongraph;

import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.runtime.util.EvictingBoundedList;

import javax.annotation.Nullable;

import java.io.Serializable;

public class ArchivedExecutionVertex implements AccessExecutionVertex, Serializable {


	private final int subTaskIndex;

	private final EvictingBoundedList<ArchivedExecution> priorExecutions;

	/** The name in the format "myTask (2/7)", cached to avoid frequent string concatenations */
	private final String taskNameWithSubtask;

	private final ArchivedExecution currentExecution;    // this field must never be null

	// ------------------------------------------------------------------------

	public ArchivedExecutionVertex(ExecutionVertex vertex) {
		this.subTaskIndex = vertex.getParallelSubtaskIndex();
		this.priorExecutions = vertex.getCopyOfPriorExecutionsList();
		this.taskNameWithSubtask = vertex.getTaskNameWithSubtaskIndex();
		this.currentExecution = vertex.getCurrentExecutionAttempt().archive();
	}

	public ArchivedExecutionVertex(
			int subTaskIndex, String taskNameWithSubtask,
			ArchivedExecution currentExecution, EvictingBoundedList<ArchivedExecution> priorExecutions) {
		this.subTaskIndex = subTaskIndex;
		this.taskNameWithSubtask = taskNameWithSubtask;
		this.currentExecution = currentExecution;
		this.priorExecutions = priorExecutions;
	}

	// --------------------------------------------------------------------------------------------
	//   Accessors
	// --------------------------------------------------------------------------------------------

	@Override
	public String getTaskNameWithSubtaskIndex() {
		return this.taskNameWithSubtask;
	}

	@Override
	public int getParallelSubtaskIndex() {
		return this.subTaskIndex;
	}

	@Override
	public ArchivedExecution getCurrentExecutionAttempt() {
		return currentExecution;
	}

	@Override
	public ExecutionState getExecutionState() {
		return currentExecution.getState();
	}

	@Override
	public long getStateTimestamp(ExecutionState state) {
		return currentExecution.getStateTimestamp(state);
	}

	@Override
	public String getFailureCauseAsString() {
		return currentExecution.getFailureCauseAsString();
	}

	@Override
	public TaskManagerLocation getCurrentAssignedResourceLocation() {
		return currentExecution.getAssignedResourceLocation();
	}

	@Nullable
	@Override
	public ArchivedExecution getPriorExecutionAttempt(int attemptNumber) {
		if (attemptNumber >= 0 && attemptNumber < priorExecutions.size()) {
			return priorExecutions.get(attemptNumber);
		} 
	}
}
