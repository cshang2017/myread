

package org.apache.flink.runtime.executiongraph.failover.flip1;

import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;

import java.util.Collections;
import java.util.Set;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Result containing the tasks to restart upon a task failure.
 * Also contains the reason if the failure is not recoverable(non-recoverable
 * failure type or restarting suppressed by restart strategy).
 */
public class FailureHandlingResult {

	/** Task vertices to restart to recover from the failure. */
	private final Set<ExecutionVertexID> verticesToRestart;

	/** Delay before the restarting can be conducted. */
	private final long restartDelayMS;

	/** Reason why the failure is not recoverable. */
	private final Throwable error;

	/** True if the original failure was a global failure. **/
	private final boolean globalFailure;

	/**
	 * Creates a result of a set of tasks to restart to recover from the failure.
	 *
	 * @param verticesToRestart containing task vertices to restart to recover from the failure
	 * @param restartDelayMS indicate a delay before conducting the restart
	 */
	private FailureHandlingResult(Set<ExecutionVertexID> verticesToRestart, long restartDelayMS, boolean globalFailure) {
		checkState(restartDelayMS >= 0);

		this.verticesToRestart = Collections.unmodifiableSet(checkNotNull(verticesToRestart));
		this.restartDelayMS = restartDelayMS;
		this.error = null;
		this.globalFailure = globalFailure;
	}

	/**
	 * Creates a result that the failure is not recoverable and no restarting should be conducted.
	 *
	 * @param error reason why the failure is not recoverable
	 */
	private FailureHandlingResult(Throwable error, boolean globalFailure) {
		this.verticesToRestart = null;
		this.restartDelayMS = -1;
		this.error = checkNotNull(error);
		this.globalFailure = globalFailure;
	}

	/**
	 * Returns the tasks to restart.
	 *
	 * @return the tasks to restart
	 */
	public Set<ExecutionVertexID> getVerticesToRestart() {
		if (canRestart()) {
			return verticesToRestart;
		} else {
			throw new IllegalStateException("Cannot get vertices to restart when the restarting is suppressed.");
		}
	}

	/**
	 * Returns the delay before the restarting.
	 *
	 * @return the delay before the restarting
	 */
	public long getRestartDelayMS() {
		if (canRestart()) {
			return restartDelayMS;
		} else {
			throw new IllegalStateException("Cannot get restart delay when the restarting is suppressed.");
		}
	}

	/**
	 * Returns reason why the restarting cannot be conducted.
	 *
	 * @return reason why the restarting cannot be conducted
	 */
	public Throwable getError() {
		if (canRestart()) {
			throw new IllegalStateException("Cannot get error when the restarting is accepted.");
		} else {
			return error;
		}
	}

	/**
	 * Returns whether the restarting can be conducted.
	 *
	 * @return whether the restarting can be conducted
	 */
	public boolean canRestart() {
		return error == null;
	}

	/**
	 * Checks if this failure was a global failure, i.e., coming from a "safety net" failover that involved
	 * all tasks and should reset also components like the coordinators.
	 */
	public boolean isGlobalFailure() {
		return globalFailure;
	}

	/**
	 * Creates a result of a set of tasks to restart to recover from the failure.
	 *
	 * <p>The result can be flagged to be from a global failure triggered by the scheduler, rather than from
	 * the failure of an individual task.
	 *
	 * @param verticesToRestart containing task vertices to restart to recover from the failure
	 * @param restartDelayMS indicate a delay before conducting the restart
	 * @return result of a set of tasks to restart to recover from the failure
	 */
	public static FailureHandlingResult restartable(
			Set<ExecutionVertexID> verticesToRestart,
			long restartDelayMS,
			boolean globalFailure) {
		return new FailureHandlingResult(verticesToRestart, restartDelayMS, globalFailure);
	}

	/**
	 * Creates a result that the failure is not recoverable and no restarting should be conducted.
	 *
	 * <p>The result can be flagged to be from a global failure triggered by the scheduler, rather than from
	 * the failure of an individual task.
	 *
	 * @param error reason why the failure is not recoverable
	 * @return result indicating the failure is not recoverable
	 */
	public static FailureHandlingResult unrecoverable(Throwable error, boolean globalFailure) {
		return new FailureHandlingResult(error, globalFailure);
	}
}
