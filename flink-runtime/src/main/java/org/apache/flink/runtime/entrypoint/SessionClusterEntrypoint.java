package org.apache.flink.runtime.entrypoint;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ConfigurationUtils;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.runtime.concurrent.ScheduledExecutor;
import org.apache.flink.runtime.dispatcher.ArchivedExecutionGraphStore;
import org.apache.flink.runtime.dispatcher.FileArchivedExecutionGraphStore;

import org.apache.flink.shaded.guava18.com.google.common.base.Ticker;

import java.io.File;
import java.io.IOException;

/**
 * Base class for session cluster entry points.
 */
public abstract class SessionClusterEntrypoint extends ClusterEntrypoint {

	public SessionClusterEntrypoint(Configuration configuration) {
		super(configuration);
	}

	@Override
	protected ArchivedExecutionGraphStore createSerializableExecutionGraphStore(
			Configuration configuration,
			ScheduledExecutor scheduledExecutor) throws IOException {
		final File tmpDir = new File(ConfigurationUtils.parseTempDirectories(configuration)[0]);

		final Time expirationTime =  Time.seconds(configuration.getLong(JobManagerOptions.JOB_STORE_EXPIRATION_TIME));
		final int maximumCapacity = configuration.getInteger(JobManagerOptions.JOB_STORE_MAX_CAPACITY);
		final long maximumCacheSizeBytes = configuration.getLong(JobManagerOptions.JOB_STORE_CACHE_SIZE);

		return new FileArchivedExecutionGraphStore(
			tmpDir,
			expirationTime,
			maximumCapacity,
			maximumCacheSizeBytes,
			scheduledExecutor,
			Ticker.systemTicker());
	}
}
