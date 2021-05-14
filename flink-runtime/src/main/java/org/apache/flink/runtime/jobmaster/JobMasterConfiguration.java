package org.apache.flink.runtime.jobmaster;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ConfigurationUtils;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.registration.RetryingRegistrationConfiguration;
import org.apache.flink.util.Preconditions;

/**
 * Configuration for the {@link JobMaster}.
 */
public class JobMasterConfiguration {

	private final Time rpcTimeout;

	private final Time slotRequestTimeout;

	private final String tmpDirectory;

	private final RetryingRegistrationConfiguration retryingRegistrationConfiguration;

	private final Configuration configuration;

	public JobMasterConfiguration(
			Time rpcTimeout,
			Time slotRequestTimeout,
			String tmpDirectory,
			RetryingRegistrationConfiguration retryingRegistrationConfiguration,
			Configuration configuration) {
		this.rpcTimeout = Preconditions.checkNotNull(rpcTimeout);
		this.slotRequestTimeout = Preconditions.checkNotNull(slotRequestTimeout);
		this.tmpDirectory = Preconditions.checkNotNull(tmpDirectory);
		this.retryingRegistrationConfiguration = retryingRegistrationConfiguration;
		this.configuration = Preconditions.checkNotNull(configuration);
	}

	public Time getRpcTimeout() {
		return rpcTimeout;
	}

	public Time getSlotRequestTimeout() {
		return slotRequestTimeout;
	}

	public String getTmpDirectory() {
		return tmpDirectory;
	}

	public RetryingRegistrationConfiguration getRetryingRegistrationConfiguration() {
		return retryingRegistrationConfiguration;
	}

	public Configuration getConfiguration() {
		return configuration;
	}

	public static JobMasterConfiguration fromConfiguration(Configuration configuration) {

		final Time rpcTimeout = AkkaUtils.getTimeoutAsTime(configuration);

		final Time slotRequestTimeout = Time.milliseconds(configuration.getLong(JobManagerOptions.SLOT_REQUEST_TIMEOUT));

		final String tmpDirectory = ConfigurationUtils.parseTempDirectories(configuration)[0];

		final RetryingRegistrationConfiguration retryingRegistrationConfiguration = RetryingRegistrationConfiguration.fromConfiguration(configuration);

		return new JobMasterConfiguration(
			rpcTimeout,
			slotRequestTimeout,
			tmpDirectory,
			retryingRegistrationConfiguration,
			configuration);
	}
}
