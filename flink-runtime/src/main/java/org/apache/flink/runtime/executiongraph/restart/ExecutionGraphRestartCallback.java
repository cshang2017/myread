package org.apache.flink.runtime.executiongraph.restart;

import org.apache.flink.runtime.executiongraph.ExecutionGraph;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A {@link RestartCallback} that abstracts restart calls on an {@link ExecutionGraph}.
 *
 * <p>This callback implementation is one-shot; it can only be used once.
 */
public class ExecutionGraphRestartCallback implements RestartCallback {

	/** The ExecutionGraph to restart. */
	private final ExecutionGraph execGraph;

	/** Atomic flag to make sure this is used only once. */
	private final AtomicBoolean used;

	/** The globalModVersion that the ExecutionGraph needs to have for the restart to go through. */
	private final long expectedGlobalModVersion;

	/**
	 * Creates a new ExecutionGraphRestartCallback.
	 *
	 * @param execGraph The ExecutionGraph to restart
	 * @param expectedGlobalModVersion  The globalModVersion that the ExecutionGraph needs to have
	 *                                  for the restart to go through
	 */
	public ExecutionGraphRestartCallback(ExecutionGraph execGraph, long expectedGlobalModVersion) {
		this.execGraph = checkNotNull(execGraph);
		this.used = new AtomicBoolean(false);
		this.expectedGlobalModVersion = expectedGlobalModVersion;
	}

	@Override
	public void triggerFullRecovery() {
		if (used.compareAndSet(false, true)) {
			execGraph.restart(expectedGlobalModVersion);
		}
	}
}
