package org.apache.flink.runtime.rest;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.blob.TransientBlobService;
import org.apache.flink.runtime.dispatcher.DispatcherGateway;
import org.apache.flink.runtime.leaderelection.LeaderElectionService;
import org.apache.flink.runtime.resourcemanager.ResourceManagerGateway;
import org.apache.flink.runtime.rest.handler.RestHandlerConfiguration;
import org.apache.flink.runtime.rest.handler.legacy.DefaultExecutionGraphCache;
import org.apache.flink.runtime.rest.handler.legacy.ExecutionGraphCache;
import org.apache.flink.runtime.rest.handler.legacy.metrics.MetricFetcher;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.runtime.webmonitor.WebMonitorEndpoint;
import org.apache.flink.runtime.webmonitor.retriever.LeaderGatewayRetriever;

import java.util.concurrent.ScheduledExecutorService;

/**
 * {@link WebMonitorEndpoint} factory.
 *
 * @param <T> type of the {@link RestfulGateway}
 */
public interface RestEndpointFactory<T extends RestfulGateway> {

	WebMonitorEndpoint<T> createRestEndpoint(
		Configuration configuration,
		LeaderGatewayRetriever<DispatcherGateway> dispatcherGatewayRetriever,
		LeaderGatewayRetriever<ResourceManagerGateway> resourceManagerGatewayRetriever,
		TransientBlobService transientBlobService,
		ScheduledExecutorService executor,
		MetricFetcher metricFetcher,
		LeaderElectionService leaderElectionService,
		FatalErrorHandler fatalErrorHandler) throws Exception;

	static ExecutionGraphCache createExecutionGraphCache(RestHandlerConfiguration restConfiguration) {
		return new DefaultExecutionGraphCache(
			restConfiguration.getTimeout(),
			Time.milliseconds(restConfiguration.getRefreshInterval()));
	}
}
