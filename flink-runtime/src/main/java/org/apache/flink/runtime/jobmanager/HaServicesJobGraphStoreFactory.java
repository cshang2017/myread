package org.apache.flink.runtime.jobmanager;

import org.apache.flink.runtime.highavailability.HighAvailabilityServices;

/**
 * {@link JobGraphStoreFactory} implementation which creates a {@link JobGraphStore}
 * using the provided {@link HighAvailabilityServices}.
 */
public class HaServicesJobGraphStoreFactory implements JobGraphStoreFactory {
	private final HighAvailabilityServices highAvailabilityServices;

	public HaServicesJobGraphStoreFactory(HighAvailabilityServices highAvailabilityServices) {
		this.highAvailabilityServices = highAvailabilityServices;
	}

	@Override
	public JobGraphStore create() {
			return highAvailabilityServices.getJobGraphStore();
	}
}
