package org.apache.flink.api.common.restartstrategy;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.configuration.RestartStrategyOptions;

import java.io.Serializable;
import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import lombok.Getter;
import lombok.AllArgsConstructor;

/**
 * This class defines methods to generate RestartStrategyConfigurations. These configurations are
 * used to create RestartStrategies at runtime.
 *
 * <p>The RestartStrategyConfigurations are used to decouple the core module from the runtime module.
 */
@PublicEvolving
public class RestartStrategies {

	/**
	 * Generates NoRestartStrategyConfiguration.
	 *
	 * @return NoRestartStrategyConfiguration
	 */
	public static RestartStrategyConfiguration noRestart() {
		return new NoRestartStrategyConfiguration();
	}

	public static RestartStrategyConfiguration fallBackRestart() {
		return new FallbackRestartStrategyConfiguration();
	}

	/**
	 * Generates a FixedDelayRestartStrategyConfiguration.
	 *
	 * @param restartAttempts Number of restart attempts for the FixedDelayRestartStrategy
	 * @param delayBetweenAttempts Delay in-between restart attempts for the FixedDelayRestartStrategy
	 * @return FixedDelayRestartStrategy
	 */
	public static RestartStrategyConfiguration fixedDelayRestart(int restartAttempts, long delayBetweenAttempts) {
		return fixedDelayRestart(restartAttempts, Time.of(delayBetweenAttempts, TimeUnit.MILLISECONDS));
	}

	/**
	 * Generates a FixedDelayRestartStrategyConfiguration.
	 *
	 * @param restartAttempts Number of restart attempts for the FixedDelayRestartStrategy
	 * @param delayInterval Delay in-between restart attempts for the FixedDelayRestartStrategy
	 * @return FixedDelayRestartStrategy
	 */
	public static RestartStrategyConfiguration fixedDelayRestart(int restartAttempts, Time delayInterval) {
		return new FixedDelayRestartStrategyConfiguration(restartAttempts, delayInterval);
	}

	/**
	 * Generates a FailureRateRestartStrategyConfiguration.
	 *
	 * @param failureRate Maximum number of restarts in given interval {@code failureInterval} before failing a job
	 * @param failureInterval Time interval for failures
	 * @param delayInterval Delay in-between restart attempts
	 */
	public static FailureRateRestartStrategyConfiguration failureRateRestart(
			int failureRate, Time failureInterval, Time delayInterval) {
		return new FailureRateRestartStrategyConfiguration(failureRate, failureInterval, delayInterval);
	}

	/**
	 * Abstract configuration for restart strategies.
	 */
	public abstract static class RestartStrategyConfiguration implements Serializable {

		private RestartStrategyConfiguration() {}

		/**
		 * Returns a description which is shown in the web interface.
		 *
		 * @return Description of the restart strategy
		 */
		public abstract String getDescription();

		@Override
		public String toString() {
			return getDescription();
		}
	}

	/**
	 * Configuration representing no restart strategy.
	 */
	public static final class NoRestartStrategyConfiguration extends RestartStrategyConfiguration {

		@Override
		public String getDescription() {
			return "Restart deactivated.";
		}
		
	}

	/**
	 * Configuration representing a fixed delay restart strategy.
	 */
	@Getter
	@AllArgsConstructor
	public static final class FixedDelayRestartStrategyConfiguration extends RestartStrategyConfiguration {

		private final int restartAttempts;
		private final Time delayBetweenAttemptsInterval;

		@Override
		public String getDescription() {
			return String.format(
				"Restart with fixed delay (%s). #%d restart attempts.",
				delayBetweenAttemptsInterval,
				restartAttempts);
		}
	}

	/**
	 * Configuration representing a failure rate restart strategy.
	 */
	public static final class FailureRateRestartStrategyConfiguration extends RestartStrategyConfiguration {
		private final int maxFailureRate;

		private final Time failureInterval;
		private final Time delayBetweenAttemptsInterval;

		public FailureRateRestartStrategyConfiguration(
				int maxFailureRate,
				Time failureInterval,
				Time delayBetweenAttemptsInterval) {
			this.maxFailureRate = maxFailureRate;
			this.failureInterval = failureInterval;
			this.delayBetweenAttemptsInterval = delayBetweenAttemptsInterval;
		}

		public int getMaxFailureRate() {
			return maxFailureRate;
		}

		public Time getFailureInterval() {
			return failureInterval;
		}

		public Time getDelayBetweenAttemptsInterval() {
			return delayBetweenAttemptsInterval;
		}

		@Override
		public String getDescription() {
			return String.format(
				"Failure rate restart with maximum of %d failures within interval %s and fixed delay %s.",
				maxFailureRate,
				failureInterval.toString(),
				delayBetweenAttemptsInterval.toString());
		}

	}

	/**
	 * Restart strategy configuration that could be used by jobs to use cluster level restart
	 * strategy. Useful especially when one has a custom implementation of restart strategy set via
	 * flink-conf.yaml.
	 */
	public static final class FallbackRestartStrategyConfiguration extends RestartStrategyConfiguration {

		@Override
		public String getDescription() {
			return "Cluster level default restart strategy";
		}

	}

	/**
	 * Reads a {@link RestartStrategyConfiguration} from a given {@link ReadableConfig}.
	 *
	 * @param configuration configuration object to retrieve parameters from
	 * @return {@link Optional#empty()} when no restart strategy parameters provided
	 */
	public static Optional<RestartStrategyConfiguration> fromConfiguration(ReadableConfig configuration) {
		return configuration.getOptional(RestartStrategyOptions.RESTART_STRATEGY)
			.map(confName -> parseConfiguration(confName, configuration));
	}

	private static RestartStrategyConfiguration parseConfiguration(
			String restartstrategyKind,
			ReadableConfig configuration) {
		switch (restartstrategyKind.toLowerCase()) {
			case "none":
			case "off":
			case "disable":
				return noRestart();
			case "fixeddelay":
			case "fixed-delay":
				int attempts = configuration.get(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_ATTEMPTS);
				Duration delay = configuration.get(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_DELAY);
				return fixedDelayRestart(attempts, delay.toMillis());
			case "failurerate":
			case "failure-rate":
				int maxFailures = configuration.get(RestartStrategyOptions.RESTART_STRATEGY_FAILURE_RATE_MAX_FAILURES_PER_INTERVAL);
				Duration failureRateInterval = configuration.get(RestartStrategyOptions.RESTART_STRATEGY_FAILURE_RATE_FAILURE_RATE_INTERVAL);
				Duration failureRateDelay = configuration.get(RestartStrategyOptions.RESTART_STRATEGY_FAILURE_RATE_DELAY);
				return failureRateRestart(
					maxFailures,
					Time.milliseconds(failureRateInterval.toMillis()),
					Time.milliseconds(failureRateDelay.toMillis()));
			default:
				throw new IllegalArgumentException("Unknown restart strategy " + restartstrategyKind + ".");
		}
	}
}
