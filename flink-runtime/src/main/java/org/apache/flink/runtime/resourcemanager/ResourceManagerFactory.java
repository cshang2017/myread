

package org.apache.flink.runtime.resourcemanager;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.ResourceIDRetrievable;
import org.apache.flink.runtime.entrypoint.ClusterInformation;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.metrics.MetricRegistry;
import org.apache.flink.runtime.metrics.groups.ResourceManagerMetricGroup;
import org.apache.flink.runtime.metrics.groups.SlotManagerMetricGroup;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.util.ConfigurationException;

import javax.annotation.Nullable;

/**
 * {@link ResourceManager} factory.
 *
 * @param <T> type of the workers of the ResourceManager
 */
public abstract class ResourceManagerFactory<T extends ResourceIDRetrievable> {

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

		final ResourceManagerMetricGroup resourceManagerMetricGroup = ResourceManagerMetricGroup.create(metricRegistry, hostname);
		final SlotManagerMetricGroup slotManagerMetricGroup = SlotManagerMetricGroup.create(metricRegistry, hostname);

		final ResourceManagerRuntimeServices resourceManagerRuntimeServices = createResourceManagerRuntimeServices(
			configuration, rpcService, highAvailabilityServices, slotManagerMetricGroup);

		return createResourceManager(
			configuration,
			resourceId,
			rpcService,
			highAvailabilityServices,
			heartbeatServices,
			fatalErrorHandler,
			clusterInformation,
			webInterfaceUrl,
			resourceManagerMetricGroup,
			resourceManagerRuntimeServices);
	}

	protected abstract ResourceManager<T> createResourceManager(
			Configuration configuration,
			ResourceID resourceId,
			RpcService rpcService,
			HighAvailabilityServices highAvailabilityServices,
			HeartbeatServices heartbeatServices,
			FatalErrorHandler fatalErrorHandler,
			ClusterInformation clusterInformation,
			@Nullable String webInterfaceUrl,
			ResourceManagerMetricGroup resourceManagerMetricGroup,
			ResourceManagerRuntimeServices resourceManagerRuntimeServices) throws Exception;

	private ResourceManagerRuntimeServices createResourceManagerRuntimeServices(
			Configuration configuration,
			RpcService rpcService,
			HighAvailabilityServices highAvailabilityServices,
			SlotManagerMetricGroup slotManagerMetricGroup) throws ConfigurationException {

		return ResourceManagerRuntimeServices.fromConfiguration(
			createResourceManagerRuntimeServicesConfiguration(configuration),
			highAvailabilityServices,
			rpcService.getScheduledExecutor(),
			slotManagerMetricGroup);
	}

	protected abstract ResourceManagerRuntimeServicesConfiguration createResourceManagerRuntimeServicesConfiguration(
			Configuration configuration) throws ConfigurationException;
}
