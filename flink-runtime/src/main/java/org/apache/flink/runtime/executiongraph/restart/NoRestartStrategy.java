package org.apache.flink.runtime.executiongraph.restart;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.concurrent.ScheduledExecutor;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;

import java.util.concurrent.CompletableFuture;

/**
 * Restart strategy which does not restart an {@link ExecutionGraph}.
 */
public class NoRestartStrategy implements RestartStrategy {

	@Override
	public boolean canRestart() {
		return false;
	}

	@Override
	public CompletableFuture<Void> restart(RestartCallback restarter, ScheduledExecutor executor) {
		throw new UnsupportedOperationException("NoRestartStrategy does not support restart.");
	}

	/**
	 * Creates a NoRestartStrategyFactory instance.
	 *
	 * @param configuration Configuration object which is ignored
	 * @return NoRestartStrategyFactory instance
	 */
	public static NoRestartStrategyFactory createFactory(Configuration configuration) {
		return new NoRestartStrategyFactory();
	}

	@Override
	public String toString() {
		return "NoRestartStrategy";
	}

	public static class NoRestartStrategyFactory extends RestartStrategyFactory {

		@Override
		public RestartStrategy createRestartStrategy() {
			return new NoRestartStrategy();
		}
	}
}
