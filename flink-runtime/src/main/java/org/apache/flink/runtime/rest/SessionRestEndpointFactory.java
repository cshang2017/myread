package org.apache.flink.runtime.rest;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.blob.TransientBlobService;
import org.apache.flink.runtime.dispatcher.DispatcherGateway;
import org.apache.flink.runtime.dispatcher.DispatcherRestEndpoint;
import org.apache.flink.runtime.leaderelection.LeaderElectionService;
import org.apache.flink.runtime.resourcemanager.ResourceManagerGateway;
import org.apache.flink.runtime.rest.handler.RestHandlerConfiguration;
import org.apache.flink.runtime.rest.handler.legacy.metrics.MetricFetcher;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.webmonitor.WebMonitorEndpoint;
import org.apache.flink.runtime.webmonitor.retriever.LeaderGatewayRetriever;

import java.util.concurrent.ScheduledExecutorService;

/**
 * {@link RestEndpointFactory} which creates a {@link DispatcherRestEndpoint}.
 */
public enum SessionRestEndpointFactory implements RestEndpointFactory<DispatcherGateway> {
	INSTANCE;

	@Override
	public WebMonitorEndpoint<DispatcherGateway> createRestEndpoint(
			Configuration configuration,
			LeaderGatewayRetriever<DispatcherGateway> dispatcherGatewayRetriever,
			LeaderGatewayRetriever<ResourceManagerGateway> resourceManagerGatewayRetriever,
			TransientBlobService transientBlobService,
			ScheduledExecutorService executor,
			MetricFetcher metricFetcher,
			LeaderElectionService leaderElectionService,
			FatalErrorHandler fatalErrorHandler) throws Exception {
		final RestHandlerConfiguration restHandlerConfiguration = RestHandlerConfiguration.fromConfiguration(configuration);

		return new DispatcherRestEndpoint(
			RestServerEndpointConfiguration.fromConfiguration(configuration),
			dispatcherGatewayRetriever,
			configuration,
			restHandlerConfiguration,
			resourceManagerGatewayRetriever,
			transientBlobService,
			executor,
			metricFetcher,
			leaderElectionService,
			RestEndpointFactory.createExecutionGraphCache(restHandlerConfiguration),
			fatalErrorHandler);
	}
}
