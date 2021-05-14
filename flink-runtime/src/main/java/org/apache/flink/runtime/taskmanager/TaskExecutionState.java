package org.apache.flink.runtime.taskmanager;

import org.apache.flink.runtime.accumulators.AccumulatorSnapshot;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.executiongraph.IOMetrics;
import org.apache.flink.util.SerializedThrowable;

import java.io.Serializable;

/**
 * This class represents an update about a task's execution state.
 *
 * <b>NOTE:</b> The exception that may be attached to the state update is
 * not necessarily a Flink or core Java exception, but may be an exception
 * from the user code. As such, it cannot be deserialized without a
 * special class loader. For that reason, the class keeps the actual
 * exception field transient and deserialized it lazily, with the
 * appropriate class loader.
 */
public class TaskExecutionState implements Serializable {

	private final JobID jobID;

	private final ExecutionAttemptID executionId;

	private final ExecutionState executionState;

	private final SerializedThrowable throwable;

	/** Serialized user-defined accumulators */
	private final AccumulatorSnapshot accumulators;

	private final IOMetrics ioMetrics;

	/**
	 * Creates a new task execution state update, with no attached exception and no accumulators.
	 *
	 * @param jobID
	 *        the ID of the job the task belongs to
	 * @param executionId
	 *        the ID of the task execution whose state is to be reported
	 * @param executionState
	 *        the execution state to be reported
	 */
	public TaskExecutionState(JobID jobID, ExecutionAttemptID executionId, ExecutionState executionState) {
		this(jobID, executionId, executionState, null, null, null);
	}

	/**
	 * Creates a new task execution state update, with an attached exception but no accumulators.
	 *
	 * @param jobID
	 *        the ID of the job the task belongs to
	 * @param executionId
	 *        the ID of the task execution whose state is to be reported
	 * @param executionState
	 *        the execution state to be reported
	 */
	public TaskExecutionState(JobID jobID, ExecutionAttemptID executionId,
							ExecutionState executionState, Throwable error) {
		this(jobID, executionId, executionState, error, null, null);
	}

	/**
	 * Creates a new task execution state update, with an attached exception.
	 * This constructor may never throw an exception.
	 *
	 * @param jobID
	 *        the ID of the job the task belongs to
	 * @param executionId
	 *        the ID of the task execution whose state is to be reported
	 * @param executionState
	 *        the execution state to be reported
	 * @param error
	 *        an optional error
	 * @param accumulators
	 *        The flink and user-defined accumulators which may be null.
	 */
	public TaskExecutionState(JobID jobID, ExecutionAttemptID executionId,
			ExecutionState executionState, Throwable error,
			AccumulatorSnapshot accumulators, IOMetrics ioMetrics) {


		this.jobID = jobID;
		this.executionId = executionId;
		this.executionState = executionState;
		if (error != null) {
			this.throwable = new SerializedThrowable(error);
		} else {
			this.throwable = null;
		}
		this.accumulators = accumulators;
		this.ioMetrics = ioMetrics;
	}

	// --------------------------------------------------------------------------------------------

	/**
	 * Gets the attached exception, which is in serialized form. Returns null,
	 * if the status update is no failure with an associated exception.
	 * 
	 * @param userCodeClassloader The classloader that can resolve user-defined exceptions.
	 * @return The attached exception, or null, if none.
	 */
	public Throwable getError(ClassLoader userCodeClassloader) {
		if (this.throwable == null) {
			return null;
		}
		else {
			return this.throwable.deserializeError(userCodeClassloader);
		}
	}

	/**
	 * Returns the ID of the task this result belongs to
	 * 
	 * @return the ID of the task this result belongs to
	 */
	public ExecutionAttemptID getID() {
		return this.executionId;
	}

	/**
	 * Returns the new execution state of the task.
	 * 
	 * @return the new execution state of the task
	 */
	public ExecutionState getExecutionState() {
		return this.executionState;
	}

	/**
	 * The ID of the job the task belongs to
	 * 
	 * @return the ID of the job the task belongs to
	 */
	public JobID getJobID() {
		return this.jobID;
	}

	/**
	 * Gets flink and user-defined accumulators in serialized form.
	 */
	public AccumulatorSnapshot getAccumulators() {
		return accumulators;
	}

	public IOMetrics getIOMetrics() {
		return ioMetrics;
	}

}
