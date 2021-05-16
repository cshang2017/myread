package org.apache.flink.runtime.rest.handler.job;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.executiongraph.AccessExecutionGraph;
import org.apache.flink.runtime.executiongraph.AccessExecutionJobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.rest.handler.legacy.ExecutionGraphCache;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.JobIDPathParameter;
import org.apache.flink.runtime.rest.messages.JobVertexIdPathParameter;
import org.apache.flink.runtime.rest.messages.JobVertexMessageParameters;
import org.apache.flink.runtime.rest.messages.MessageHeaders;
import org.apache.flink.runtime.rest.messages.ResponseBody;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

import java.util.Map;
import java.util.concurrent.Executor;

/**
 * Base class for request handlers whose response depends on a specific job vertex (defined
 * via the "{@link JobVertexIdPathParameter#KEY}" parameter) in a specific job,
 * defined via (defined via the "{@link JobIDPathParameter#KEY}" parameter).
 *
 * @param <R> the response type
 * @param <M> the message parameters type
 */
public abstract class AbstractJobVertexHandler<R extends ResponseBody, M extends JobVertexMessageParameters> extends AbstractExecutionGraphHandler<R, M> {

	/**
	 * Instantiates a new Abstract job vertex handler.
	 *
	 * @param leaderRetriever     the leader retriever
	 * @param timeout             the timeout
	 * @param responseHeaders     the response headers
	 * @param messageHeaders      the message headers
	 * @param executionGraphCache the execution graph cache
	 * @param executor            the executor
	 */
	protected AbstractJobVertexHandler(
			GatewayRetriever<? extends RestfulGateway> leaderRetriever,
			Time timeout,
			Map<String, String> responseHeaders,
			MessageHeaders<EmptyRequestBody, R, M> messageHeaders,
			ExecutionGraphCache executionGraphCache,
			Executor executor) {

		super(leaderRetriever, timeout, responseHeaders, messageHeaders, executionGraphCache, executor);
	}

	@Override
	protected R handleRequest(
			HandlerRequest<EmptyRequestBody, M> request,
			AccessExecutionGraph executionGraph) throws RestHandlerException {

		final JobVertexID jobVertexID = request.getPathParameter(JobVertexIdPathParameter.class);
		final AccessExecutionJobVertex jobVertex = executionGraph.getJobVertex(jobVertexID);

		return handleRequest(request, jobVertex);
	}

	/**
	 * Called for each request after the corresponding {@link AccessExecutionJobVertex} has been retrieved from the
	 * {@link AccessExecutionGraph}.
	 *
	 * @param request   the request
	 * @param jobVertex the execution job vertex
	 * @return the response
	 * @throws RestHandlerException if the handler could not process the request
	 */
	protected abstract R handleRequest(HandlerRequest<EmptyRequestBody, M> request, AccessExecutionJobVertex jobVertex) throws RestHandlerException;
}
