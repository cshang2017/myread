
package org.apache.flink.runtime.resourcemanager;

import org.apache.flink.runtime.concurrent.ScheduledExecutor;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.metrics.groups.SlotManagerMetricGroup;
import org.apache.flink.runtime.resourcemanager.slotmanager.SlotManager;
import org.apache.flink.runtime.resourcemanager.slotmanager.SlotManagerImpl;
import org.apache.flink.util.Preconditions;

/**
 * Container class for the {@link ResourceManager} services.
 */
public class ResourceManagerRuntimeServices {

	private final SlotManager slotManager;
	private final JobLeaderIdService jobLeaderIdService;

	public ResourceManagerRuntimeServices(SlotManager slotManager, JobLeaderIdService jobLeaderIdService) {
		this.slotManager = Preconditions.checkNotNull(slotManager);
		this.jobLeaderIdService = Preconditions.checkNotNull(jobLeaderIdService);
	}

	public SlotManager getSlotManager() {
		return slotManager;
	}

	public JobLeaderIdService getJobLeaderIdService() {
		return jobLeaderIdService;
	}

	// -------------------- Static methods --------------------------------------

	public static ResourceManagerRuntimeServices fromConfiguration(
			ResourceManagerRuntimeServicesConfiguration configuration,
			HighAvailabilityServices highAvailabilityServices,
			ScheduledExecutor scheduledExecutor,
			SlotManagerMetricGroup slotManagerMetricGroup) {

		final SlotManager slotManager = new SlotManagerImpl(
			scheduledExecutor,
			configuration.getSlotManagerConfiguration(),
			slotManagerMetricGroup);

		final JobLeaderIdService jobLeaderIdService = new JobLeaderIdService(
			highAvailabilityServices,
			scheduledExecutor,
			configuration.getJobTimeout());

		return new ResourceManagerRuntimeServices(slotManager, jobLeaderIdService);
	}
}
