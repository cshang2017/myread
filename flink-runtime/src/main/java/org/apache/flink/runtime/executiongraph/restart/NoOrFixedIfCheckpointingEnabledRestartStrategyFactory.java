package org.apache.flink.runtime.executiongraph.restart;

import java.time.Duration;

/**
 * Default restart strategy that resolves either to {@link NoRestartStrategy} or {@link FixedDelayRestartStrategy}
 * depending if checkpointing was enabled.
 */
public class NoOrFixedIfCheckpointingEnabledRestartStrategyFactory extends RestartStrategyFactory {
	public static final int DEFAULT_RESTART_ATTEMPTS = Integer.MAX_VALUE;

	public static final long DEFAULT_RESTART_DELAY = Duration.ofSeconds(1L).toMillis();

	@Override
	public RestartStrategy createRestartStrategy() {
		return createRestartStrategy(false);
	}

	RestartStrategy createRestartStrategy(boolean isCheckpointingEnabled) {
		if (isCheckpointingEnabled) {
			return new FixedDelayRestartStrategy(DEFAULT_RESTART_ATTEMPTS, DEFAULT_RESTART_DELAY);
		} else {
			return new NoRestartStrategy();
		}
	}
}
