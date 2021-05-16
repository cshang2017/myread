package org.apache.flink.runtime.rest.handler;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.WebOptions;
import org.apache.flink.util.Preconditions;

import java.io.File;

/**
 * Configuration object containing values for the rest handler configuration.
 */
public class RestHandlerConfiguration {

	private final long refreshInterval;

	private final int maxCheckpointStatisticCacheEntries;

	private final Time timeout;

	private final File webUiDir;

	private final boolean webSubmitEnabled;

	public RestHandlerConfiguration(
			long refreshInterval,
			int maxCheckpointStatisticCacheEntries,
			Time timeout,
			File webUiDir,
			boolean webSubmitEnabled) {
		Preconditions.checkArgument(refreshInterval > 0L, "The refresh interval (ms) should be larger than 0.");
		this.refreshInterval = refreshInterval;

		this.maxCheckpointStatisticCacheEntries = maxCheckpointStatisticCacheEntries;

		this.timeout = Preconditions.checkNotNull(timeout);
		this.webUiDir = Preconditions.checkNotNull(webUiDir);
		this.webSubmitEnabled = webSubmitEnabled;
	}

	public long getRefreshInterval() {
		return refreshInterval;
	}

	public int getMaxCheckpointStatisticCacheEntries() {
		return maxCheckpointStatisticCacheEntries;
	}

	public Time getTimeout() {
		return timeout;
	}

	public File getWebUiDir() {
		return webUiDir;
	}

	public boolean isWebSubmitEnabled() {
		return webSubmitEnabled;
	}

	public static RestHandlerConfiguration fromConfiguration(Configuration configuration) {
		final long refreshInterval = configuration.getLong(WebOptions.REFRESH_INTERVAL);

		final int maxCheckpointStatisticCacheEntries = configuration.getInteger(WebOptions.CHECKPOINTS_HISTORY_SIZE);

		final Time timeout = Time.milliseconds(configuration.getLong(WebOptions.TIMEOUT));

		final String rootDir = "flink-web-ui";
		final File webUiDir = new File(configuration.getString(WebOptions.TMP_DIR), rootDir);

		final boolean webSubmitEnabled = configuration.getBoolean(WebOptions.SUBMIT_ENABLE);

		return new RestHandlerConfiguration(
			refreshInterval,
			maxCheckpointStatisticCacheEntries,
			timeout,
			webUiDir,
			webSubmitEnabled);
	}
}
