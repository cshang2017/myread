
package org.apache.flink.runtime.executiongraph.restart;

/**
 * A callback to trigger restarts, passed to the {@link RestartStrategy} to
 * trigger recovery on the ExecutionGraph. 
 */
public interface RestartCallback {

	/**
	 * Triggers a full recovery in the target ExecutionGraph.
	 * A full recovery resets all vertices to the state of the latest checkpoint.
	 */
	void triggerFullRecovery();
}
