
package org.apache.flink.runtime.executiongraph.failover;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.util.StringUtils;

import org.slf4j.Logger;

import javax.annotation.Nullable;

/**
 * A utility class to load failover strategies from the configuration.
 */
public class FailoverStrategyLoader {

	/** Config name for the {@link RestartAllStrategy}. */
	public static final String FULL_RESTART_STRATEGY_NAME = "full";

	// ------------------------------------------------------------------------

	/**
	 * Loads a FailoverStrategy Factory from the given configuration.
	 */
	public static FailoverStrategy.Factory loadFailoverStrategy(Configuration config, @Nullable Logger logger) {
		final String strategyParam = config.getString(
			JobManagerOptions.EXECUTION_FAILOVER_STRATEGY,
			FULL_RESTART_STRATEGY_NAME);

		if (StringUtils.isNullOrWhitespaceOnly(strategyParam)) {
			if (logger != null) {
				logger.warn("Null config value for {} ; using default failover strategy (full restarts).",
						JobManagerOptions.EXECUTION_FAILOVER_STRATEGY.key());
			}

			return new RestartAllStrategy.Factory();
		}
		else {
			switch (strategyParam.toLowerCase()) {
				case FULL_RESTART_STRATEGY_NAME:
					return new RestartAllStrategy.Factory();

				default:
					// we could interpret the parameter as a factory class name and instantiate that
					// for now we simply do not support this
					throw new IllegalConfigurationException("Unknown failover strategy: " + strategyParam);
			}
		}
	}
}
