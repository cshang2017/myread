package org.apache.flink.runtime.rest.handler.taskmanager;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.blob.TransientBlobKey;
import org.apache.flink.runtime.blob.TransientBlobService;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.resourcemanager.ResourceManagerGateway;
import org.apache.flink.runtime.resourcemanager.exceptions.UnknownTaskExecutorException;
import org.apache.flink.runtime.rest.NotFoundException;
import org.apache.flink.runtime.rest.handler.AbstractHandler;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.rest.handler.util.HandlerUtils;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.UntypedResponseMessageHeaders;
import org.apache.flink.runtime.rest.messages.taskmanager.TaskManagerIdPathParameter;
import org.apache.flink.runtime.rest.messages.taskmanager.TaskManagerMessageParameters;
import org.apache.flink.runtime.taskexecutor.TaskExecutor;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.guava18.com.google.common.cache.CacheBuilder;
import org.apache.flink.shaded.guava18.com.google.common.cache.CacheLoader;
import org.apache.flink.shaded.guava18.com.google.common.cache.LoadingCache;
import org.apache.flink.shaded.guava18.com.google.common.cache.RemovalNotification;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpRequest;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

import javax.annotation.Nonnull;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * Base class for serving files from the {@link TaskExecutor}.
 */
public abstract class AbstractTaskManagerFileHandler<M extends TaskManagerMessageParameters> extends AbstractHandler<RestfulGateway, EmptyRequestBody, M> {

	private final GatewayRetriever<ResourceManagerGateway> resourceManagerGatewayRetriever;
	private final TransientBlobService transientBlobService;

	private final LoadingCache<Tuple2<ResourceID, String>, CompletableFuture<TransientBlobKey>> fileBlobKeys;

	protected AbstractTaskManagerFileHandler(
			@Nonnull GatewayRetriever<? extends RestfulGateway> leaderRetriever,
			@Nonnull Time timeout,
			@Nonnull Map<String, String> responseHeaders,
			@Nonnull UntypedResponseMessageHeaders<EmptyRequestBody, M> untypedResponseMessageHeaders,
			@Nonnull GatewayRetriever<ResourceManagerGateway> resourceManagerGatewayRetriever,
			@Nonnull TransientBlobService transientBlobService,
			@Nonnull Time cacheEntryDuration) {
		super(leaderRetriever, timeout, responseHeaders, untypedResponseMessageHeaders);

		this.resourceManagerGatewayRetriever = Preconditions.checkNotNull(resourceManagerGatewayRetriever);

		this.transientBlobService = Preconditions.checkNotNull(transientBlobService);

		this.fileBlobKeys = CacheBuilder
			.newBuilder()
			.expireAfterWrite(cacheEntryDuration.toMilliseconds(), TimeUnit.MILLISECONDS)
			.removalListener(this::removeBlob)
			.build(
				new CacheLoader<Tuple2<ResourceID, String>, CompletableFuture<TransientBlobKey>>() {
					@Override
					public CompletableFuture<TransientBlobKey> load(Tuple2<ResourceID, String> taskManagerIdAndFileName) {
						return loadTaskManagerFile(taskManagerIdAndFileName);
					}
			});
	}

	@Override
	protected CompletableFuture<Void> respondToRequest(ChannelHandlerContext ctx, HttpRequest httpRequest, HandlerRequest<EmptyRequestBody, M> handlerRequest, RestfulGateway gateway) {
		final ResourceID taskManagerId = handlerRequest.getPathParameter(TaskManagerIdPathParameter.class);

		String filename = getFileName(handlerRequest);
		final Tuple2<ResourceID, String> taskManagerIdAndFileName = new Tuple2<>(taskManagerId, filename);
		final CompletableFuture<TransientBlobKey> blobKeyFuture = fileBlobKeys.get(taskManagerIdAndFileName);
		

		final CompletableFuture<Void> resultFuture = blobKeyFuture.thenAcceptAsync(
			(TransientBlobKey blobKey) -> {
				File file = transientBlobService.getFile(blobKey);
				HandlerUtils.transferFile(
						ctx,
						file,
						httpRequest);
			},
			ctx.executor());

		return resultFuture.whenComplete(
			(Void ignored, Throwable throwable) -> {
				if (throwable != null) {
					fileBlobKeys.invalidate(taskManagerId);
					final Throwable strippedThrowable = ExceptionUtils.stripCompletionException(throwable);
				}
			});
	}

	private CompletableFuture<TransientBlobKey> loadTaskManagerFile(Tuple2<ResourceID, String> taskManagerIdAndFileName) throws RestHandlerException {
		final ResourceManagerGateway resourceManagerGateway = resourceManagerGatewayRetriever
			.getNow()
			.orElseThrow(() -> {
				return new RestHandlerException(
					"Cannot connect to ResourceManager right now. Please try to refresh.",
					HttpResponseStatus.NOT_FOUND);
			});

		return requestFileUpload(resourceManagerGateway, taskManagerIdAndFileName);
	}

	protected abstract CompletableFuture<TransientBlobKey> requestFileUpload(ResourceManagerGateway resourceManagerGateway, Tuple2<ResourceID, String> taskManagerIdAndFileName);

	private void removeBlob(RemovalNotification<Tuple2<ResourceID, String>, CompletableFuture<TransientBlobKey>> removalNotification) {
		final CompletableFuture<TransientBlobKey> value = removalNotification.getValue();

		if (value != null) {
			value.thenAccept(transientBlobService::deleteFromCache);
		}
	}

	protected String getFileName(HandlerRequest<EmptyRequestBody, M> handlerRequest) {
		return null;
	}
}
