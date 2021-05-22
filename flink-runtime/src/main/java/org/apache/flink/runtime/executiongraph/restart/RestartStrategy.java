
package org.apache.flink.runtime.executiongraph.restart;

import org.apache.flink.runtime.concurrent.ScheduledExecutor;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;

import java.util.concurrent.CompletableFuture;

/**
 * Strategy for {@link ExecutionGraph} restarts.
 */
public interface RestartStrategy {

	/**
	 * True if the restart strategy can be applied to restart the {@link ExecutionGraph}.
	 *
	 * @return true if restart is possible, otherwise false
	 */
	boolean canRestart();

	/**
	 * Called by the ExecutionGraph to eventually trigger a full recovery.
	 * The recovery must be triggered on the given callback object, and may be delayed
	 * with the help of the given scheduled executor.
	 *
	 * <p>The thread that calls this method is not supposed to block/sleep.
	 *
	 * @param restarter The hook to restart the ExecutionGraph
	 * @param executor An scheduled executor to delay the restart
	 * @return A {@link CompletableFuture} that will be completed when the restarting process is done.
	 */
	CompletableFuture<Void> restart(RestartCallback restarter, ScheduledExecutor executor);
}
