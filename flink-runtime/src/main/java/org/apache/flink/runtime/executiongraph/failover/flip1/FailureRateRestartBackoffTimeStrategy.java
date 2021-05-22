package org.apache.flink.runtime.executiongraph.failover.flip1;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.util.clock.Clock;
import org.apache.flink.util.clock.SystemClock;

import java.util.ArrayDeque;
import java.util.Deque;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Restart strategy which can restart when failure rate is not exceeded.
 */
public class FailureRateRestartBackoffTimeStrategy implements RestartBackoffTimeStrategy {

	private final long failuresIntervalMS;

	private final long backoffTimeMS;

	private final int maxFailuresPerInterval;

	private final Deque<Long> failureTimestamps;

	private final String strategyString;

	private final Clock clock;

	FailureRateRestartBackoffTimeStrategy(Clock clock, int maxFailuresPerInterval, long failuresIntervalMS, long backoffTimeMS) {

		checkArgument(maxFailuresPerInterval > 0, "Maximum number of restart attempts per time unit must be greater than 0.");
		checkArgument(failuresIntervalMS > 0, "Failures interval must be greater than 0 ms.");
		checkArgument(backoffTimeMS >= 0, "Backoff time must be at least 0 ms.");

		this.failuresIntervalMS = failuresIntervalMS;
		this.backoffTimeMS = backoffTimeMS;
		this.maxFailuresPerInterval = maxFailuresPerInterval;
		this.failureTimestamps = new ArrayDeque<>(maxFailuresPerInterval);
		this.strategyString = generateStrategyString();
		this.clock = checkNotNull(clock);
	}

	@Override
	public boolean canRestart() {
		if (isFailureTimestampsQueueFull()) {
			Long now = clock.absoluteTimeMillis();
			Long earliestFailure = failureTimestamps.peek();

			return (now - earliestFailure) > failuresIntervalMS;
		} else {
			return true;
		}
	}

	@Override
	public long getBackoffTime() {
		return backoffTimeMS;
	}

	@Override
	public void notifyFailure(Throwable cause) {
		if (isFailureTimestampsQueueFull()) {
			failureTimestamps.remove();
		}
		failureTimestamps.add(clock.absoluteTimeMillis());
	}

	@Override
	public String toString() {
		return strategyString;
	}

	private boolean isFailureTimestampsQueueFull() {
		return failureTimestamps.size() >= maxFailuresPerInterval;
	}

	private String generateStrategyString() {
		StringBuilder str = new StringBuilder("FailureRateRestartBackoffTimeStrategy(");
		str.append("FailureRateRestartBackoffTimeStrategy(failuresIntervalMS=");
		str.append(failuresIntervalMS);
		str.append(",backoffTimeMS=");
		str.append(backoffTimeMS);
		str.append(",maxFailuresPerInterval=");
		str.append(maxFailuresPerInterval);
		str.append(")");

		return str.toString();
	}

	public static FailureRateRestartBackoffTimeStrategyFactory createFactory(final Configuration configuration) {
		int maxFailuresPerInterval = configuration.getInteger(RestartStrategyOptions.RESTART_STRATEGY_FAILURE_RATE_MAX_FAILURES_PER_INTERVAL);
		long failuresInterval = configuration.get(RestartStrategyOptions.RESTART_STRATEGY_FAILURE_RATE_FAILURE_RATE_INTERVAL)
			.toMillis();
		long delay = configuration.get(RestartStrategyOptions.RESTART_STRATEGY_FAILURE_RATE_DELAY).toMillis();

		return new FailureRateRestartBackoffTimeStrategyFactory(maxFailuresPerInterval, failuresInterval, delay);
	}

	/**
	 * The factory for creating {@link FailureRateRestartBackoffTimeStrategy}.
	 */
	public static class FailureRateRestartBackoffTimeStrategyFactory implements RestartBackoffTimeStrategy.Factory {

		private final int maxFailuresPerInterval;

		private final long failuresIntervalMS;

		private final long backoffTimeMS;

		public FailureRateRestartBackoffTimeStrategyFactory(
				int maxFailuresPerInterval,
				long failuresIntervalMS,
				long backoffTimeMS) {

			this.maxFailuresPerInterval = maxFailuresPerInterval;
			this.failuresIntervalMS = failuresIntervalMS;
			this.backoffTimeMS = backoffTimeMS;
		}

		@Override
		public RestartBackoffTimeStrategy create() {
			return new FailureRateRestartBackoffTimeStrategy(
				SystemClock.getInstance(),
				maxFailuresPerInterval,
				failuresIntervalMS,
				backoffTimeMS);
		}
	}
}
