package org.apache.flink.runtime.executiongraph.restart;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Utility method for resolving {@link RestartStrategy}.
 */
public final class RestartStrategyResolving {

	/**
	 * Resolves which {@link RestartStrategy} to use. It should be used only on the server side.
	 * The resolving strategy is as follows:
	 * <ol>
	 * <li>Strategy set within job graph.</li>
	 * <li>Strategy set flink-conf.yaml on the server set, unless is set to {@link NoRestartStrategy} and checkpointing
	 * is enabled.</li>
	 * <li>If no strategy was set on client and server side and checkpointing was enabled then
	 * {@link FixedDelayRestartStrategy} is used</li>
	 * </ol>
	 *
	 * @param clientConfiguration restart configuration given within the job graph
	 * @param serverStrategyFactory default server side strategy factory
	 * @param isCheckpointingEnabled if checkpointing was enabled for the job
	 * @return resolved strategy
	 */
	public static RestartStrategy resolve(
			RestartStrategies.RestartStrategyConfiguration clientConfiguration,
			RestartStrategyFactory serverStrategyFactory,
			boolean isCheckpointingEnabled) {

		checkNotNull(serverStrategyFactory);

		final RestartStrategy clientSideRestartStrategy =
			RestartStrategyFactory.createRestartStrategy(clientConfiguration);

		if (clientSideRestartStrategy != null) {
			return clientSideRestartStrategy;
		} else {
			if (serverStrategyFactory instanceof NoOrFixedIfCheckpointingEnabledRestartStrategyFactory) {
				return ((NoOrFixedIfCheckpointingEnabledRestartStrategyFactory) serverStrategyFactory)
					.createRestartStrategy(isCheckpointingEnabled);
			} else {
				return serverStrategyFactory.createRestartStrategy();
			}
		}
	}

	private RestartStrategyResolving() {
	}
}
