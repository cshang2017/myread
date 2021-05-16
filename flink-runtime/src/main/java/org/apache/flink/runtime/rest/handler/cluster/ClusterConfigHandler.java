package org.apache.flink.runtime.rest.handler.cluster;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.rest.handler.AbstractRestHandler;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.rest.messages.ClusterConfigurationInfo;
import org.apache.flink.runtime.rest.messages.EmptyMessageParameters;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.MessageHeaders;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nonnull;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Handler which serves the cluster's configuration.
 */
public class ClusterConfigHandler extends AbstractRestHandler<RestfulGateway, EmptyRequestBody, ClusterConfigurationInfo, EmptyMessageParameters> {

	private final ClusterConfigurationInfo clusterConfig;

	public ClusterConfigHandler(
			GatewayRetriever<? extends RestfulGateway> leaderRetriever,
			Time timeout,
			Map<String, String> responseHeaders,
			MessageHeaders<EmptyRequestBody, ClusterConfigurationInfo, EmptyMessageParameters> messageHeaders,
			Configuration configuration) {
		super(leaderRetriever, timeout, responseHeaders, messageHeaders);

		Preconditions.checkNotNull(configuration);
		this.clusterConfig = ClusterConfigurationInfo.from(configuration);
	}

	@Override
	protected CompletableFuture<ClusterConfigurationInfo> handleRequest(@Nonnull HandlerRequest<EmptyRequestBody, EmptyMessageParameters> request, @Nonnull RestfulGateway gateway) throws RestHandlerException {
		return CompletableFuture.completedFuture(clusterConfig);
	}
}
