

package org.apache.flink.runtime.executiongraph.failover.flip1.partitionrelease;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;

/**
 * Instantiates a {@link RegionPartitionReleaseStrategy}.
 */
public final class PartitionReleaseStrategyFactoryLoader {

	public static PartitionReleaseStrategy.Factory loadPartitionReleaseStrategyFactory(final Configuration configuration) {
		final boolean partitionReleaseDuringJobExecution = configuration.getBoolean(JobManagerOptions.PARTITION_RELEASE_DURING_JOB_EXECUTION);
		if (partitionReleaseDuringJobExecution) {
			return new RegionPartitionReleaseStrategy.Factory();
		} else {
			return new NotReleasingPartitionReleaseStrategy.Factory();
		}
	}

	private PartitionReleaseStrategyFactoryLoader() {
	}
}
