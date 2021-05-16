package org.apache.flink.runtime.rest.handler;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.rest.FileUploadHandler;
import org.apache.flink.runtime.rest.FlinkHttpObjectAggregator;
import org.apache.flink.runtime.rest.handler.router.RoutedRequest;
import org.apache.flink.runtime.rest.handler.util.HandlerUtils;
import org.apache.flink.runtime.rest.messages.ErrorResponseBody;
import org.apache.flink.runtime.rest.messages.MessageParameters;
import org.apache.flink.runtime.rest.messages.RequestBody;
import org.apache.flink.runtime.rest.messages.UntypedResponseMessageHeaders;
import org.apache.flink.runtime.rest.util.RestMapperUtils;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;
import org.apache.flink.util.AutoCloseableAsync;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.guava18.com.google.common.base.Ascii;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonParseException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonMappingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBufInputStream;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.FullHttpRequest;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpRequest;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

/**
 * Super class for netty-based handlers that work with {@link RequestBody}.
 *
 * <p>Subclasses must be thread-safe
 *
 * @param <T> type of the leader gateway
 * @param <R> type of the incoming request
 * @param <M> type of the message parameters
 */
public abstract class AbstractHandler<T extends RestfulGateway, R extends RequestBody, M extends MessageParameters> extends LeaderRetrievalHandler<T> implements AutoCloseableAsync {

	protected static final ObjectMapper MAPPER = RestMapperUtils.getStrictObjectMapper();

	/**
	 * Other response payload overhead (in bytes).
	 * If we truncate response payload, we should leave enough buffer for this overhead.
	 */
	private static final int OTHER_RESP_PAYLOAD_OVERHEAD = 1024;

	private final UntypedResponseMessageHeaders<R, M> untypedResponseMessageHeaders;

	/**
	 * Used to ensure that the handler is not closed while there are still in-flight requests.
	 */
	private final InFlightRequestTracker inFlightRequestTracker;

	/**
	 * CompletableFuture object that ensures calls to {@link #closeAsync()} idempotent.
	 */
	private CompletableFuture<Void> terminationFuture;

	/**
	 * Lock object to prevent concurrent calls to {@link #closeAsync()}.
	 */
	private final Object lock = new Object();

	protected AbstractHandler(
			@Nonnull GatewayRetriever<? extends T> leaderRetriever,
			@Nonnull Time timeout,
			@Nonnull Map<String, String> responseHeaders,
			@Nonnull UntypedResponseMessageHeaders<R, M> untypedResponseMessageHeaders) {
		super(leaderRetriever, timeout, responseHeaders);

		this.untypedResponseMessageHeaders = Preconditions.checkNotNull(untypedResponseMessageHeaders);
		this.inFlightRequestTracker = new InFlightRequestTracker();
	}

	@Override
	protected void respondAsLeader(ChannelHandlerContext ctx, RoutedRequest routedRequest, T gateway) {
		HttpRequest httpRequest = routedRequest.getRequest();

		FileUploads uploadedFiles = null;
		if (!inFlightRequestTracker.registerRequest()) {
			ctx.channel().close();
			return;
		}

		final ByteBuf msgContent = ((FullHttpRequest) httpRequest).content();

		uploadedFiles = FileUploadHandler.getMultipartFileUploads(ctx);

		if (!untypedResponseMessageHeaders.acceptsFileUploads() && !uploadedFiles.getUploadedFiles().isEmpty()) {
			throw new RestHandlerException("File uploads not allowed.", HttpResponseStatus.BAD_REQUEST);
		}

		R request;
		if (msgContent.capacity() == 0) {
			request = MAPPER.readValue("{}", untypedResponseMessageHeaders.getRequestClass());
		} else {
			InputStream in = new ByteBufInputStream(msgContent);
			request = MAPPER.readValue(in, untypedResponseMessageHeaders.getRequestClass());
		}

		final HandlerRequest<R, M> handlerRequest = new HandlerRequest<R, M>(
				request,
				untypedResponseMessageHeaders.getUnresolvedMessageParameters(),
				routedRequest.getRouteResult().pathParams(),
				routedRequest.getRouteResult().queryParams(),
				uploadedFiles.getUploadedFiles());

		CompletableFuture<Void> requestProcessingFuture = respondToRequest(
			ctx,
			httpRequest,
			handlerRequest,
			gateway);

		final FileUploads finalUploadedFiles = uploadedFiles;
		requestProcessingFuture
			.handle((Void ignored, Throwable throwable) -> {
				if (throwable != null) {
					return handleException(ExceptionUtils.stripCompletionException(throwable), ctx, httpRequest);
				}
				return CompletableFuture.<Void>completedFuture(null);
			}).thenCompose(Function.identity())
			.whenComplete((Void ignored, Throwable throwable) -> {
				finalizeRequestProcessing(finalUploadedFiles);
			});
	}

	private void finalizeRequestProcessing(FileUploads uploadedFiles) {
		inFlightRequestTracker.deregisterRequest();
		cleanupFileUploads(uploadedFiles);
	}

	private CompletableFuture<Void> handleException(Throwable throwable, ChannelHandlerContext ctx, HttpRequest httpRequest) {
		FlinkHttpObjectAggregator flinkHttpObjectAggregator = ctx.pipeline().get(FlinkHttpObjectAggregator.class);
		if (flinkHttpObjectAggregator == null) {
			return CompletableFuture.completedFuture(null);
		}
		int maxLength = flinkHttpObjectAggregator.maxContentLength() - OTHER_RESP_PAYLOAD_OVERHEAD;
		if (throwable instanceof RestHandlerException) {
			RestHandlerException rhe = (RestHandlerException) throwable;
			String stackTrace = ExceptionUtils.stringifyException(rhe);
			String truncatedStackTrace = Ascii.truncate(stackTrace, maxLength, "...");
			
			return HandlerUtils.sendErrorResponse(
				ctx,
				httpRequest,
				new ErrorResponseBody(truncatedStackTrace),
				rhe.getHttpResponseStatus(),
				responseHeaders);
		} else {
			String stackTrace = String.format("<Exception on server side:%n%s%nEnd of exception on server side>",
				ExceptionUtils.stringifyException(throwable));
			String truncatedStackTrace = Ascii.truncate(stackTrace, maxLength, "...");
			return HandlerUtils.sendErrorResponse(
				ctx,
				httpRequest,
				new ErrorResponseBody(Arrays.asList("Internal server error.", truncatedStackTrace)),
				HttpResponseStatus.INTERNAL_SERVER_ERROR,
				responseHeaders);
		}
	}

	@Override
	public final CompletableFuture<Void> closeAsync() {
		synchronized (lock) {
			if (terminationFuture == null) {
				this.terminationFuture = FutureUtils.composeAfterwards(closeHandlerAsync(), inFlightRequestTracker::awaitAsync);
			}
			return this.terminationFuture;
		}
	}

	protected CompletableFuture<Void> closeHandlerAsync() {
		return CompletableFuture.completedFuture(null);
	}

	private void cleanupFileUploads(@Nullable FileUploads uploadedFiles) {
		if (uploadedFiles != null) {
			uploadedFiles.close();
		}
	}

	/**
	 * Respond to the given {@link HandlerRequest}.
	 *
	 * @param ctx channel handler context to write the response
	 * @param httpRequest original http request
	 * @param handlerRequest typed handler request
	 * @param gateway leader gateway
	 * @return Future which is completed once the request has been processed
	 * @throws RestHandlerException if an exception occurred while responding
	 */
	protected abstract CompletableFuture<Void> respondToRequest(
		ChannelHandlerContext ctx,
		HttpRequest httpRequest,
		HandlerRequest<R, M> handlerRequest,
		T gateway) throws RestHandlerException;
}
