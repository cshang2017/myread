package org.apache.flink.runtime.jobmaster;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.blob.TransientBlobService;
import org.apache.flink.runtime.entrypoint.JobClusterEntrypoint;
import org.apache.flink.runtime.leaderelection.LeaderElectionService;
import org.apache.flink.runtime.resourcemanager.ResourceManagerGateway;
import org.apache.flink.runtime.rest.RestServerEndpointConfiguration;
import org.apache.flink.runtime.rest.handler.RestHandlerConfiguration;
import org.apache.flink.runtime.rest.handler.legacy.ExecutionGraphCache;
import org.apache.flink.runtime.rest.handler.legacy.metrics.MetricFetcher;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.runtime.webmonitor.WebMonitorEndpoint;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;

import java.io.IOException;
import java.util.concurrent.ScheduledExecutorService;

/**
 * REST endpoint for the {@link JobClusterEntrypoint}.
 */
public class MiniDispatcherRestEndpoint extends WebMonitorEndpoint<RestfulGateway> {

	public MiniDispatcherRestEndpoint(
			RestServerEndpointConfiguration endpointConfiguration,
			GatewayRetriever<? extends RestfulGateway> leaderRetriever,
			Configuration clusterConfiguration,
			RestHandlerConfiguration restConfiguration,
			GatewayRetriever<ResourceManagerGateway> resourceManagerRetriever,
			TransientBlobService transientBlobService,
			ScheduledExecutorService executor,
			MetricFetcher metricFetcher,
			LeaderElectionService leaderElectionService,
			ExecutionGraphCache executionGraphCache,
			FatalErrorHandler fatalErrorHandler) throws IOException {
		super(
			endpointConfiguration,
			leaderRetriever,
			clusterConfiguration,
			restConfiguration,
			resourceManagerRetriever,
			transientBlobService,
			executor,
			metricFetcher,
			leaderElectionService,
			executionGraphCache,
			fatalErrorHandler);
	}
}
