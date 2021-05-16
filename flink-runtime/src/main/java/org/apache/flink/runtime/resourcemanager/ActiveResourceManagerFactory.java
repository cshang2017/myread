package org.apache.flink.runtime.resourcemanager;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.runtime.clusterframework.TaskExecutorProcessUtils;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.ResourceIDRetrievable;
import org.apache.flink.runtime.entrypoint.ClusterInformation;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.metrics.MetricRegistry;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.RpcService;

import javax.annotation.Nullable;

/**
 * Resource manager factory which creates active {@link ResourceManager} implementations.
 *
 * <p>The default implementation will call {@link #createActiveResourceManagerConfiguration}
 * to create a new configuration which is configured with active resource manager relevant
 * configuration options.
 *
 * @param <T> type of the {@link ResourceIDRetrievable}
 */
public abstract class ActiveResourceManagerFactory<T extends ResourceIDRetrievable> extends ResourceManagerFactory<T> {

	@Override
	public ResourceManager<T> createResourceManager(
			Configuration configuration,
			ResourceID resourceId,
			RpcService rpcService,
			HighAvailabilityServices highAvailabilityServices,
			HeartbeatServices heartbeatServices,
			FatalErrorHandler fatalErrorHandler,
			ClusterInformation clusterInformation,
			@Nullable String webInterfaceUrl,
			MetricRegistry metricRegistry,
			String hostname) throws Exception {
		return super.createResourceManager(
			createActiveResourceManagerConfiguration(configuration),
			resourceId,
			rpcService,
			highAvailabilityServices,
			heartbeatServices,
			fatalErrorHandler,
			clusterInformation,
			webInterfaceUrl,
			metricRegistry,
			hostname);
	}

	private Configuration createActiveResourceManagerConfiguration(Configuration originalConfiguration) {
		return TaskExecutorProcessUtils.getConfigurationMapLegacyTaskManagerHeapSizeToConfigOption(
			originalConfiguration, TaskManagerOptions.TOTAL_PROCESS_MEMORY);
	}
}
