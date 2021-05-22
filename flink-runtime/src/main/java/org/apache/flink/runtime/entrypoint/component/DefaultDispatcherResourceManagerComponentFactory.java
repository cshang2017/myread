package org.apache.flink.runtime.entrypoint.component;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MetricOptions;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.runtime.blob.BlobServer;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.dispatcher.ArchivedExecutionGraphStore;
import org.apache.flink.runtime.dispatcher.DispatcherGateway;
import org.apache.flink.runtime.dispatcher.DispatcherId;
import org.apache.flink.runtime.dispatcher.HistoryServerArchivist;
import org.apache.flink.runtime.dispatcher.PartialDispatcherServices;
import org.apache.flink.runtime.dispatcher.SessionDispatcherFactory;
import org.apache.flink.runtime.dispatcher.runner.DefaultDispatcherRunnerFactory;
import org.apache.flink.runtime.dispatcher.runner.DispatcherRunner;
import org.apache.flink.runtime.dispatcher.runner.DispatcherRunnerFactory;
import org.apache.flink.runtime.entrypoint.ClusterInformation;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.jobmanager.HaServicesJobGraphStoreFactory;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalService;
import org.apache.flink.runtime.metrics.MetricRegistry;
import org.apache.flink.runtime.metrics.util.MetricUtils;
import org.apache.flink.runtime.resourcemanager.ResourceManager;
import org.apache.flink.runtime.resourcemanager.ResourceManagerFactory;
import org.apache.flink.runtime.resourcemanager.ResourceManagerGateway;
import org.apache.flink.runtime.resourcemanager.ResourceManagerId;
import org.apache.flink.runtime.rest.JobRestEndpointFactory;
import org.apache.flink.runtime.rest.RestEndpointFactory;
import org.apache.flink.runtime.rest.SessionRestEndpointFactory;
import org.apache.flink.runtime.rest.handler.legacy.metrics.MetricFetcher;
import org.apache.flink.runtime.rest.handler.legacy.metrics.MetricFetcherImpl;
import org.apache.flink.runtime.rest.handler.legacy.metrics.VoidMetricFetcher;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.rpc.RpcUtils;
import org.apache.flink.runtime.webmonitor.WebMonitorEndpoint;
import org.apache.flink.runtime.webmonitor.retriever.LeaderGatewayRetriever;
import org.apache.flink.runtime.webmonitor.retriever.MetricQueryServiceRetriever;
import org.apache.flink.runtime.webmonitor.retriever.impl.RpcGatewayRetriever;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkException;

import javax.annotation.Nonnull;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Abstract class which implements the creation of the {@link DispatcherResourceManagerComponent} components.
 */
public class DefaultDispatcherResourceManagerComponentFactory implements DispatcherResourceManagerComponentFactory {

	@Nonnull
	private final DispatcherRunnerFactory dispatcherRunnerFactory;

	@Nonnull
	private final ResourceManagerFactory<?> resourceManagerFactory;

	@Nonnull
	private final RestEndpointFactory<?> restEndpointFactory;

	public DefaultDispatcherResourceManagerComponentFactory(
			@Nonnull DispatcherRunnerFactory dispatcherRunnerFactory,
			@Nonnull ResourceManagerFactory<?> resourceManagerFactory,
			@Nonnull RestEndpointFactory<?> restEndpointFactory) {
		this.dispatcherRunnerFactory = dispatcherRunnerFactory;
		this.resourceManagerFactory = resourceManagerFactory;
		this.restEndpointFactory = restEndpointFactory;
	}

	@Override
	public DispatcherResourceManagerComponent create(
			Configuration configuration,
			Executor ioExecutor,
			RpcService rpcService,
			HighAvailabilityServices highAvailabilityServices,
			BlobServer blobServer,
			HeartbeatServices heartbeatServices,
			MetricRegistry metricRegistry,
			ArchivedExecutionGraphStore archivedExecutionGraphStore,
			MetricQueryServiceRetriever metricQueryServiceRetriever,
			FatalErrorHandler fatalErrorHandler) throws Exception {

		LeaderRetrievalService dispatcherLeaderRetrievalService = null;
		LeaderRetrievalService resourceManagerRetrievalService = null;
		WebMonitorEndpoint<?> webMonitorEndpoint = null;
		ResourceManager<?> resourceManager = null;
		DispatcherRunner dispatcherRunner = null;

			dispatcherLeaderRetrievalService = highAvailabilityServices.getDispatcherLeaderRetriever();

			resourceManagerRetrievalService = highAvailabilityServices.getResourceManagerLeaderRetriever();

			final LeaderGatewayRetriever<DispatcherGateway> dispatcherGatewayRetriever = new RpcGatewayRetriever<>(
				rpcService,
				DispatcherGateway.class,
				DispatcherId::fromUuid,
				10,
				Time.milliseconds(50L));

			final LeaderGatewayRetriever<ResourceManagerGateway> resourceManagerGatewayRetriever = new RpcGatewayRetriever<>(
				rpcService,
				ResourceManagerGateway.class,
				ResourceManagerId::fromUuid,
				10,
				Time.milliseconds(50L));

			final ScheduledExecutorService executor = WebMonitorEndpoint.createExecutorService(
				configuration.getInteger(RestOptions.SERVER_NUM_THREADS),
				configuration.getInteger(RestOptions.SERVER_THREAD_PRIORITY),
				"DispatcherRestEndpoint");

			final long updateInterval = configuration.getLong(MetricOptions.METRIC_FETCHER_UPDATE_INTERVAL);
			final MetricFetcher metricFetcher = updateInterval == 0
				? VoidMetricFetcher.INSTANCE
				: MetricFetcherImpl.fromConfiguration(
					configuration,
					metricQueryServiceRetriever,
					dispatcherGatewayRetriever,
					executor);

			webMonitorEndpoint = restEndpointFactory.createRestEndpoint(
				configuration,
				dispatcherGatewayRetriever,
				resourceManagerGatewayRetriever,
				blobServer,
				executor,
				metricFetcher,
				highAvailabilityServices.getClusterRestEndpointLeaderElectionService(),
				fatalErrorHandler);

			webMonitorEndpoint.start();

			final String hostname = RpcUtils.getHostname(rpcService);

			resourceManager = resourceManagerFactory.createResourceManager(
				configuration,
				ResourceID.generate(),
				rpcService,
				highAvailabilityServices,
				heartbeatServices,
				fatalErrorHandler,
				new ClusterInformation(hostname, blobServer.getPort()),
				webMonitorEndpoint.getRestBaseUrl(),
				metricRegistry,
				hostname);

			final HistoryServerArchivist historyServerArchivist = HistoryServerArchivist.createHistoryServerArchivist(configuration, webMonitorEndpoint, ioExecutor);

			final PartialDispatcherServices partialDispatcherServices = new PartialDispatcherServices(
				configuration,
				highAvailabilityServices,
				resourceManagerGatewayRetriever,
				blobServer,
				heartbeatServices,
				() -> MetricUtils.instantiateJobManagerMetricGroup(metricRegistry, hostname),
				archivedExecutionGraphStore,
				fatalErrorHandler,
				historyServerArchivist,
				metricRegistry.getMetricQueryServiceGatewayRpcAddress());

			dispatcherRunner = dispatcherRunnerFactory.createDispatcherRunner(
				highAvailabilityServices.getDispatcherLeaderElectionService(),
				fatalErrorHandler,
				new HaServicesJobGraphStoreFactory(highAvailabilityServices),
				ioExecutor,
				rpcService,
				partialDispatcherServices);

			resourceManager.start();

			resourceManagerRetrievalService.start(resourceManagerGatewayRetriever);
			dispatcherLeaderRetrievalService.start(dispatcherGatewayRetriever);

			return new DispatcherResourceManagerComponent(
				dispatcherRunner,
				DefaultResourceManagerService.createFor(resourceManager),
				dispatcherLeaderRetrievalService,
				resourceManagerRetrievalService,
				webMonitorEndpoint,
				fatalErrorHandler);

			
	}

	public static DefaultDispatcherResourceManagerComponentFactory createSessionComponentFactory(
			ResourceManagerFactory<?> resourceManagerFactory) {
		return new DefaultDispatcherResourceManagerComponentFactory(
			DefaultDispatcherRunnerFactory.createSessionRunner(SessionDispatcherFactory.INSTANCE),
			resourceManagerFactory,
			SessionRestEndpointFactory.INSTANCE);
	}

	public static DefaultDispatcherResourceManagerComponentFactory createJobComponentFactory(
			ResourceManagerFactory<?> resourceManagerFactory,
			JobGraphRetriever jobGraphRetriever) {
		return new DefaultDispatcherResourceManagerComponentFactory(
			DefaultDispatcherRunnerFactory.createJobRunner(jobGraphRetriever),
			resourceManagerFactory,
			JobRestEndpointFactory.INSTANCE);
	}
}
