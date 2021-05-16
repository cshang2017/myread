package org.apache.flink.runtime.rest.handler.job;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.executiongraph.AccessExecutionGraph;
import org.apache.flink.runtime.messages.FlinkJobNotFoundException;
import org.apache.flink.runtime.rest.NotFoundException;
import org.apache.flink.runtime.rest.handler.AbstractRestHandler;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.rest.handler.legacy.ExecutionGraphCache;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.JobIDPathParameter;
import org.apache.flink.runtime.rest.messages.JobMessageParameters;
import org.apache.flink.runtime.rest.messages.MessageHeaders;
import org.apache.flink.runtime.rest.messages.ResponseBody;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nonnull;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;

/**
 * Base class for all {@link AccessExecutionGraph} based REST handlers.
 *
 * @param <R> response type
 */
public abstract class AbstractExecutionGraphHandler<R extends ResponseBody, M extends JobMessageParameters> extends AbstractRestHandler<RestfulGateway, EmptyRequestBody, R, M> {

	private final ExecutionGraphCache executionGraphCache;

	private final Executor executor;

	protected AbstractExecutionGraphHandler(
			GatewayRetriever<? extends RestfulGateway> leaderRetriever,
			Time timeout,
			Map<String, String> responseHeaders,
			MessageHeaders<EmptyRequestBody, R, M> messageHeaders,
			ExecutionGraphCache executionGraphCache,
			Executor executor) {
		super(leaderRetriever, timeout, responseHeaders, messageHeaders);

		this.executionGraphCache = Preconditions.checkNotNull(executionGraphCache);
		this.executor = Preconditions.checkNotNull(executor);
	}

	@Override
	protected CompletableFuture<R> handleRequest(@Nonnull HandlerRequest<EmptyRequestBody, M> request, @Nonnull RestfulGateway gateway) throws RestHandlerException {
		JobID jobId = request.getPathParameter(JobIDPathParameter.class);

		CompletableFuture<AccessExecutionGraph> executionGraphFuture = executionGraphCache.getExecutionGraph(jobId, gateway);

		return executionGraphFuture.thenApplyAsync(
			executionGraph -> {
					return handleRequest(request, executionGraph);
			}, executor);
	}

	/**
	 * Called for each request after the corresponding {@link AccessExecutionGraph} has been retrieved from the
	 * {@link ExecutionGraphCache}.
	 *
	 * @param request for further information
	 * @param executionGraph for which the handler was called
	 * @return Response
	 * @throws RestHandlerException if the handler could not process the request
	 */
	protected abstract R handleRequest(HandlerRequest<EmptyRequestBody, M> request, AccessExecutionGraph executionGraph) throws RestHandlerException;
}
