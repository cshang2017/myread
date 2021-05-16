package org.apache.flink.runtime.rest.handler.taskmanager;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.blob.TransientBlobKey;
import org.apache.flink.runtime.blob.TransientBlobService;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.resourcemanager.ResourceManagerGateway;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.UntypedResponseMessageHeaders;
import org.apache.flink.runtime.rest.messages.taskmanager.TaskManagerMessageParameters;
import org.apache.flink.runtime.taskexecutor.FileType;
import org.apache.flink.runtime.taskexecutor.TaskExecutor;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;

import javax.annotation.Nonnull;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Rest handler which serves the stdout file of the {@link TaskExecutor}.
 */
public class TaskManagerStdoutFileHandler extends AbstractTaskManagerFileHandler<TaskManagerMessageParameters> {

	public TaskManagerStdoutFileHandler(
			@Nonnull GatewayRetriever<? extends RestfulGateway> leaderRetriever,
			@Nonnull Time timeout,
			@Nonnull Map<String, String> responseHeaders,
			@Nonnull UntypedResponseMessageHeaders<EmptyRequestBody, TaskManagerMessageParameters> untypedResponseMessageHeaders,
			@Nonnull GatewayRetriever<ResourceManagerGateway> resourceManagerGatewayRetriever,
			@Nonnull TransientBlobService transientBlobService,
			@Nonnull Time cacheEntryDuration) {
		super(leaderRetriever, timeout, responseHeaders, untypedResponseMessageHeaders, resourceManagerGatewayRetriever, transientBlobService, cacheEntryDuration);
	}

	@Override
	protected CompletableFuture<TransientBlobKey> requestFileUpload(ResourceManagerGateway resourceManagerGateway, Tuple2<ResourceID, String> taskManagerIdAndFileName) {
		return resourceManagerGateway.requestTaskManagerFileUploadByType(taskManagerIdAndFileName.f0, FileType.STDOUT, timeout);
	}
}
