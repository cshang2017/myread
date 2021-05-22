package org.apache.flink.runtime.executiongraph.restart;

import org.apache.flink.runtime.concurrent.ScheduledExecutor;

import java.util.concurrent.CompletableFuture;


/**
 * A restart strategy that validates that it is not in use by throwing {@link IllegalStateException}
 * on any method call.
 */
public class ThrowingRestartStrategy implements RestartStrategy {

	@Override
	public boolean canRestart() {
		throw new IllegalStateException("Unexpected canRestart() call");
	}

	@Override
	public CompletableFuture<Void> restart(final RestartCallback restarter, final ScheduledExecutor executor) {
		throw new IllegalStateException("Unexpected restart() call");
	}

	/**
	 * Factory for {@link ThrowingRestartStrategy}.
	 */
	public static class ThrowingRestartStrategyFactory extends RestartStrategyFactory {

		private static final long serialVersionUID = 1L;

		@Override
		public RestartStrategy createRestartStrategy() {
			return new ThrowingRestartStrategy();
		}
	}
}
