package org.apache.flink.runtime.executiongraph.restart;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.concurrent.ScheduledExecutor;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.util.Preconditions;

import java.util.concurrent.CompletableFuture;

/**
 * Restart strategy which tries to restart the given {@link ExecutionGraph} a fixed number of times
 * with a fixed time delay in between.
 */
public class FixedDelayRestartStrategy implements RestartStrategy {

	private final int maxNumberRestartAttempts;
	private final long delayBetweenRestartAttempts;
	private int currentRestartAttempt;

	public FixedDelayRestartStrategy(
		int maxNumberRestartAttempts,
		long delayBetweenRestartAttempts) {

		Preconditions.checkArgument(maxNumberRestartAttempts >= 0, "Maximum number of restart attempts must be positive.");
		Preconditions.checkArgument(delayBetweenRestartAttempts >= 0, "Delay between restart attempts must be positive");

		this.maxNumberRestartAttempts = maxNumberRestartAttempts;
		this.delayBetweenRestartAttempts = delayBetweenRestartAttempts;
		currentRestartAttempt = 0;
	}

	public int getCurrentRestartAttempt() {
		return currentRestartAttempt;
	}

	@Override
	public boolean canRestart() {
		return currentRestartAttempt < maxNumberRestartAttempts;
	}

	@Override
	public CompletableFuture<Void> restart(final RestartCallback restarter, ScheduledExecutor executor) {
		currentRestartAttempt++;
		return FutureUtils.scheduleWithDelay(restarter::triggerFullRecovery, Time.milliseconds(delayBetweenRestartAttempts), executor);
	}

	/**
	 * Creates a FixedDelayRestartStrategy from the given Configuration.
	 *
	 * @param configuration Configuration containing the parameter values for the restart strategy
	 * @return Initialized instance of FixedDelayRestartStrategy
	 * @throws Exception
	 */
	public static FixedDelayRestartStrategyFactory createFactory(Configuration configuration) throws Exception {
		int maxAttempts = configuration.getInteger(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_ATTEMPTS);
		long delay = configuration.get(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_DELAY).toMillis();

		return new FixedDelayRestartStrategyFactory(maxAttempts, delay);
	}

	@Override
	public String toString() {
		return "FixedDelayRestartStrategy(" +
				"maxNumberRestartAttempts=" + maxNumberRestartAttempts +
				", delayBetweenRestartAttempts=" + delayBetweenRestartAttempts +
				')';
	}

	public static class FixedDelayRestartStrategyFactory extends RestartStrategyFactory {

		private final int maxNumberRestartAttempts;
		private final long delayBetweenRestartAttempts;

		public FixedDelayRestartStrategyFactory(int maxNumberRestartAttempts, long delayBetweenRestartAttempts) {
			this.maxNumberRestartAttempts = maxNumberRestartAttempts;
			this.delayBetweenRestartAttempts = delayBetweenRestartAttempts;
		}

		@Override
		public RestartStrategy createRestartStrategy() {
			return new FixedDelayRestartStrategy(maxNumberRestartAttempts, delayBetweenRestartAttempts);
		}

		int getMaxNumberRestartAttempts() {
			return maxNumberRestartAttempts;
		}

		long getDelayBetweenRestartAttempts() {
			return delayBetweenRestartAttempts;
		}
	}
}
