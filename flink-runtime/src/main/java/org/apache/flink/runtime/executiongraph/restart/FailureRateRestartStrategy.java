package org.apache.flink.runtime.executiongraph.restart;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.concurrent.ScheduledExecutor;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.util.Preconditions;

import java.util.ArrayDeque;
import java.util.concurrent.CompletableFuture;

/**
 * Restart strategy which tries to restart the given {@link ExecutionGraph} when failure rate exceeded
 * with a fixed time delay in between.
 */
public class FailureRateRestartStrategy implements RestartStrategy {

	private final Time failuresInterval;
	private final Time delayInterval;
	private final int maxFailuresPerInterval;
	private final ArrayDeque<Long> restartTimestampsDeque;

	public FailureRateRestartStrategy(int maxFailuresPerInterval, Time failuresInterval, Time delayInterval) {
		Preconditions.checkNotNull(failuresInterval, "Failures interval cannot be null.");
		Preconditions.checkNotNull(delayInterval, "Delay interval cannot be null.");
		Preconditions.checkArgument(maxFailuresPerInterval > 0, "Maximum number of restart attempts per time unit must be greater than 0.");
		Preconditions.checkArgument(failuresInterval.getSize() > 0, "Failures interval must be greater than 0 ms.");
		Preconditions.checkArgument(delayInterval.getSize() >= 0, "Delay interval must be at least 0 ms.");

		this.failuresInterval = failuresInterval;
		this.delayInterval = delayInterval;
		this.maxFailuresPerInterval = maxFailuresPerInterval;
		this.restartTimestampsDeque = new ArrayDeque<>(maxFailuresPerInterval);
	}

	@Override
	public boolean canRestart() {
		if (isRestartTimestampsQueueFull()) {
			Long now = System.currentTimeMillis();
			Long earliestFailure = restartTimestampsDeque.peek();

			return (now - earliestFailure) > failuresInterval.toMilliseconds();
		} else {
			return true;
		}
	}

	@Override
	public CompletableFuture<Void> restart(final RestartCallback restarter, ScheduledExecutor executor) {
		if (isRestartTimestampsQueueFull()) {
			restartTimestampsDeque.remove();
		}
		restartTimestampsDeque.add(System.currentTimeMillis());
		return FutureUtils.scheduleWithDelay(restarter::triggerFullRecovery, delayInterval, executor);
	}

	private boolean isRestartTimestampsQueueFull() {
		return restartTimestampsDeque.size() >= maxFailuresPerInterval;
	}

	@Override
	public String toString() {
		return "FailureRateRestartStrategy(" +
			"failuresInterval=" + failuresInterval +
			"delayInterval=" + delayInterval +
			"maxFailuresPerInterval=" + maxFailuresPerInterval +
			")";
	}

	public static FailureRateRestartStrategyFactory createFactory(Configuration configuration) throws Exception {
		int maxFailuresPerInterval = configuration.getInteger(RestartStrategyOptions.RESTART_STRATEGY_FAILURE_RATE_MAX_FAILURES_PER_INTERVAL);
		long failuresInterval = configuration.get(RestartStrategyOptions.RESTART_STRATEGY_FAILURE_RATE_FAILURE_RATE_INTERVAL)
			.toMillis();
		long delay = configuration.get(RestartStrategyOptions.RESTART_STRATEGY_FAILURE_RATE_DELAY).toMillis();

		return new FailureRateRestartStrategyFactory(
			maxFailuresPerInterval,
			Time.milliseconds(failuresInterval),
			Time.milliseconds(delay));
	}

	public static class FailureRateRestartStrategyFactory extends RestartStrategyFactory {
		private static final long serialVersionUID = -373724639430960480L;

		private final int maxFailuresPerInterval;
		private final Time failuresInterval;
		private final Time delayInterval;

		public FailureRateRestartStrategyFactory(int maxFailuresPerInterval, Time failuresInterval, Time delayInterval) {
			this.maxFailuresPerInterval = maxFailuresPerInterval;
			this.failuresInterval = Preconditions.checkNotNull(failuresInterval);
			this.delayInterval = Preconditions.checkNotNull(delayInterval);
		}

		@Override
		public RestartStrategy createRestartStrategy() {
			return new FailureRateRestartStrategy(maxFailuresPerInterval, failuresInterval, delayInterval);
		}
	}
}
