package org.apache.flink.runtime.dispatcher;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.blob.TransientBlobService;
import org.apache.flink.runtime.leaderelection.LeaderElectionService;
import org.apache.flink.runtime.resourcemanager.ResourceManagerGateway;
import org.apache.flink.runtime.rest.RestServerEndpointConfiguration;
import org.apache.flink.runtime.rest.handler.RestHandlerConfiguration;
import org.apache.flink.runtime.rest.handler.RestHandlerSpecification;
import org.apache.flink.runtime.rest.handler.job.JobSubmitHandler;
import org.apache.flink.runtime.rest.handler.legacy.ExecutionGraphCache;
import org.apache.flink.runtime.rest.handler.legacy.metrics.MetricFetcher;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.webmonitor.WebMonitorEndpoint;
import org.apache.flink.runtime.webmonitor.WebMonitorExtension;
import org.apache.flink.runtime.webmonitor.WebMonitorUtils;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkException;

import org.apache.flink.shaded.netty4.io.netty.channel.ChannelInboundHandler;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;

/**
 * REST endpoint for the {@link Dispatcher} component.
 */
public class DispatcherRestEndpoint extends WebMonitorEndpoint<DispatcherGateway> {

	private WebMonitorExtension webSubmissionExtension;

	public DispatcherRestEndpoint(
			RestServerEndpointConfiguration endpointConfiguration,
			GatewayRetriever<DispatcherGateway> leaderRetriever,
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

		webSubmissionExtension = WebMonitorExtension.empty();
	}

	@Override
	protected List<Tuple2<RestHandlerSpecification, ChannelInboundHandler>> initializeHandlers(final CompletableFuture<String> localAddressFuture) {
		List<Tuple2<RestHandlerSpecification, ChannelInboundHandler>> handlers = super.initializeHandlers(localAddressFuture);

		// Add the Dispatcher specific handlers

		final Time timeout = restConfiguration.getTimeout();

		JobSubmitHandler jobSubmitHandler = new JobSubmitHandler(
			leaderRetriever,
			timeout,
			responseHeaders,
			executor,
			clusterConfiguration);

		handlers.add(Tuple2.of(jobSubmitHandler.getMessageHeaders(), jobSubmitHandler));

		return handlers;
	}

	@Override
	protected Collection<Tuple2<RestHandlerSpecification, ChannelInboundHandler>> initializeWebSubmissionHandlers(CompletableFuture<String> localAddressFuture) {
		if (restConfiguration.isWebSubmitEnabled()) {
				final Time timeout = restConfiguration.getTimeout();

				webSubmissionExtension = WebMonitorUtils.loadWebSubmissionExtension(
					leaderRetriever,
					timeout,
					responseHeaders,
					localAddressFuture,
					uploadDir,
					executor,
					clusterConfiguration);

				return webSubmissionExtension.getHandlers();
			
		}

		return Collections.emptyList();
	}

	@Override
	protected CompletableFuture<Void> shutDownInternal() {
		final CompletableFuture<Void> shutdownFuture = super.shutDownInternal();

		final CompletableFuture<Void> shutdownResultFuture = new CompletableFuture<>();

		shutdownFuture.whenComplete(
			(Void ignored, Throwable throwable) -> {
				webSubmissionExtension.closeAsync().whenComplete(
					(Void innerIgnored, Throwable innerThrowable) -> {
						if (innerThrowable != null) {
							shutdownResultFuture.completeExceptionally(
								ExceptionUtils.firstOrSuppressed(innerThrowable, throwable));
						} else if (throwable != null) {
							shutdownResultFuture.completeExceptionally(throwable);
						} else {
							shutdownResultFuture.complete(null);
						}
					});
			});

		return shutdownResultFuture;
	}
}
