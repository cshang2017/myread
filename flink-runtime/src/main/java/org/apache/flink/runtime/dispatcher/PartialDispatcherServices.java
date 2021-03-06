package org.apache.flink.runtime.dispatcher;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.blob.BlobServer;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.resourcemanager.ResourceManagerGateway;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Partial {@link DispatcherServices} services container which needs to
 * be completed before being given to the {@link Dispatcher}.
 */
public class PartialDispatcherServices {

	@Nonnull
	private final Configuration configuration;

	@Nonnull
	private final HighAvailabilityServices highAvailabilityServices;

	@Nonnull
	private final GatewayRetriever<ResourceManagerGateway> resourceManagerGatewayRetriever;

	@Nonnull
	private final BlobServer blobServer;

	@Nonnull
	private final HeartbeatServices heartbeatServices;

	@Nonnull
	private final JobManagerMetricGroupFactory jobManagerMetricGroupFactory;

	@Nonnull
	private final ArchivedExecutionGraphStore archivedExecutionGraphStore;

	@Nonnull
	private final FatalErrorHandler fatalErrorHandler;

	@Nonnull
	private final HistoryServerArchivist historyServerArchivist;

	@Nullable
	private final String metricQueryServiceAddress;

	public PartialDispatcherServices(
			@Nonnull Configuration configuration,
			@Nonnull HighAvailabilityServices highAvailabilityServices,
			@Nonnull GatewayRetriever<ResourceManagerGateway> resourceManagerGatewayRetriever,
			@Nonnull BlobServer blobServer,
			@Nonnull HeartbeatServices heartbeatServices,
			@Nonnull JobManagerMetricGroupFactory jobManagerMetricGroupFactory,
			@Nonnull ArchivedExecutionGraphStore archivedExecutionGraphStore,
			@Nonnull FatalErrorHandler fatalErrorHandler,
			@Nonnull HistoryServerArchivist historyServerArchivist,
			@Nullable String metricQueryServiceAddress) {
		this.configuration = configuration;
		this.highAvailabilityServices = highAvailabilityServices;
		this.resourceManagerGatewayRetriever = resourceManagerGatewayRetriever;
		this.blobServer = blobServer;
		this.heartbeatServices = heartbeatServices;
		this.jobManagerMetricGroupFactory = jobManagerMetricGroupFactory;
		this.archivedExecutionGraphStore = archivedExecutionGraphStore;
		this.fatalErrorHandler = fatalErrorHandler;
		this.historyServerArchivist = historyServerArchivist;
		this.metricQueryServiceAddress = metricQueryServiceAddress;
	}

	@Nonnull
	public Configuration getConfiguration() {
		return configuration;
	}

	@Nonnull
	public HighAvailabilityServices getHighAvailabilityServices() {
		return highAvailabilityServices;
	}

	@Nonnull
	public GatewayRetriever<ResourceManagerGateway> getResourceManagerGatewayRetriever() {
		return resourceManagerGatewayRetriever;
	}

	@Nonnull
	public BlobServer getBlobServer() {
		return blobServer;
	}

	@Nonnull
	public HeartbeatServices getHeartbeatServices() {
		return heartbeatServices;
	}

	@Nonnull
	public JobManagerMetricGroupFactory getJobManagerMetricGroupFactory() {
		return jobManagerMetricGroupFactory;
	}

	@Nonnull
	public ArchivedExecutionGraphStore getArchivedExecutionGraphStore() {
		return archivedExecutionGraphStore;
	}

	@Nonnull
	public FatalErrorHandler getFatalErrorHandler() {
		return fatalErrorHandler;
	}

	@Nonnull
	public HistoryServerArchivist getHistoryServerArchivist() {
		return historyServerArchivist;
	}

	@Nullable
	public String getMetricQueryServiceAddress() {
		return metricQueryServiceAddress;
	}
}
