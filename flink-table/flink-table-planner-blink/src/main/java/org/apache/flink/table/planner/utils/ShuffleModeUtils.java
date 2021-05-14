package org.apache.flink.table.planner.utils;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.graph.GlobalDataExchangeMode;
import org.apache.flink.table.api.config.ExecutionConfigOptions;

/**
 * Utility class to load job-wide shuffle mode.
 */
public class ShuffleModeUtils {

	static final String ALL_EDGES_BLOCKING_LEGACY = "batch";

	static final String ALL_EDGES_PIPELINED_LEGACY = "pipelined";

	static GlobalDataExchangeMode getShuffleModeAsGlobalDataExchangeMode(final Configuration configuration) {
		final String value = configuration.getString(ExecutionConfigOptions.TABLE_EXEC_SHUFFLE_MODE);

		try {
			return GlobalDataExchangeMode.valueOf(convertLegacyShuffleMode(value).toUpperCase());
		} catch (IllegalArgumentException e) {
			throw new IllegalArgumentException(
				String.format("Unsupported value %s for config %s.", value, ExecutionConfigOptions.TABLE_EXEC_SHUFFLE_MODE.key()));
		}
	}

	private static String convertLegacyShuffleMode(final String shuffleMode) {
		switch (shuffleMode.toLowerCase()) {
			case ALL_EDGES_BLOCKING_LEGACY:
				return GlobalDataExchangeMode.ALL_EDGES_BLOCKING.toString();
			case ALL_EDGES_PIPELINED_LEGACY:
				return GlobalDataExchangeMode.ALL_EDGES_PIPELINED.toString();
			default:
				return shuffleMode;
		}
	}
}
