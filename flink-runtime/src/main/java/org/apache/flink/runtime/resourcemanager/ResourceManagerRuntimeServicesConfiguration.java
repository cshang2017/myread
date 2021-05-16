
package org.apache.flink.runtime.resourcemanager;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ResourceManagerOptions;
import org.apache.flink.runtime.resourcemanager.slotmanager.SlotManagerConfiguration;
import org.apache.flink.util.ConfigurationException;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.TimeUtils;

/**
 * Configuration class for the {@link ResourceManagerRuntimeServices} class.
 */
public class ResourceManagerRuntimeServicesConfiguration {

	private final Time jobTimeout;

	private final SlotManagerConfiguration slotManagerConfiguration;

	public ResourceManagerRuntimeServicesConfiguration(Time jobTimeout, SlotManagerConfiguration slotManagerConfiguration) {
		this.jobTimeout = Preconditions.checkNotNull(jobTimeout);
		this.slotManagerConfiguration = Preconditions.checkNotNull(slotManagerConfiguration);
	}

	public Time getJobTimeout() {
		return jobTimeout;
	}

	public SlotManagerConfiguration getSlotManagerConfiguration() {
		return slotManagerConfiguration;
	}

	// ---------------------------- Static methods ----------------------------------

	public static ResourceManagerRuntimeServicesConfiguration fromConfiguration(
			Configuration configuration,
			WorkerResourceSpecFactory defaultWorkerResourceSpecFactory) throws ConfigurationException {

		final String strJobTimeout = configuration.getString(ResourceManagerOptions.JOB_TIMEOUT);
		final Time jobTimeout;

			jobTimeout = Time.milliseconds(TimeUtils.parseDuration(strJobTimeout).toMillis());
				final WorkerResourceSpec defaultWorkerResourceSpec = defaultWorkerResourceSpecFactory.createDefaultWorkerResourceSpec(configuration);
		final SlotManagerConfiguration slotManagerConfiguration =
			SlotManagerConfiguration.fromConfiguration(configuration, defaultWorkerResourceSpec);

		return new ResourceManagerRuntimeServicesConfiguration(jobTimeout, slotManagerConfiguration);
	}
}
