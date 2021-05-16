package org.apache.flink.runtime.rest.handler.resourcemanager;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.resourcemanager.ResourceManager;
import org.apache.flink.runtime.resourcemanager.ResourceManagerGateway;
import org.apache.flink.runtime.rest.handler.AbstractRestHandler;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.rest.messages.MessageHeaders;
import org.apache.flink.runtime.rest.messages.MessageParameters;
import org.apache.flink.runtime.rest.messages.RequestBody;
import org.apache.flink.runtime.rest.messages.ResponseBody;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

import javax.annotation.Nonnull;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Base class for REST handlers which need access to the {@link ResourceManager}.
 *
 * @param <T> type of the {@link RestfulGateway}
 * @param <R> request type
 * @param <P> response type
 * @param <M> message parameters type
 */
public abstract class AbstractResourceManagerHandler<T extends RestfulGateway, R extends RequestBody, P extends ResponseBody, M extends MessageParameters> extends AbstractRestHandler<T, R, P, M> {

	private final GatewayRetriever<ResourceManagerGateway> resourceManagerGatewayRetriever;

	protected AbstractResourceManagerHandler(
			GatewayRetriever<? extends T> leaderRetriever,
			Time timeout,
			Map<String, String> responseHeaders,
			MessageHeaders<R, P, M> messageHeaders,
			GatewayRetriever<ResourceManagerGateway> resourceManagerGatewayRetriever) {
		super(leaderRetriever, timeout, responseHeaders, messageHeaders);

		this.resourceManagerGatewayRetriever = resourceManagerGatewayRetriever;
	}

	@Override
	protected CompletableFuture<P> handleRequest(@Nonnull HandlerRequest<R, M> request, @Nonnull T gateway) throws RestHandlerException {
		ResourceManagerGateway resourceManagerGateway = getResourceManagerGateway(resourceManagerGatewayRetriever);

		return handleRequest(request, resourceManagerGateway);
	}

	protected abstract CompletableFuture<P> handleRequest(@Nonnull HandlerRequest<R, M> request, @Nonnull ResourceManagerGateway gateway) throws RestHandlerException;

	public static ResourceManagerGateway getResourceManagerGateway(GatewayRetriever<ResourceManagerGateway> resourceManagerGatewayRetriever) throws RestHandlerException {
		return resourceManagerGatewayRetriever
			.getNow()
			.orElseThrow(() -> new RestHandlerException(
				"Cannot connect to ResourceManager right now. Please try to refresh.",
				HttpResponseStatus.NOT_FOUND));
	}
}
