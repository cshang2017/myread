package org.apache.flink.runtime.rest.handler.job;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.executiongraph.AccessExecution;
import org.apache.flink.runtime.executiongraph.AccessExecutionVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.rest.handler.legacy.ExecutionGraphCache;
import org.apache.flink.runtime.rest.handler.legacy.metrics.MetricFetcher;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.JobIDPathParameter;
import org.apache.flink.runtime.rest.messages.JobVertexIdPathParameter;
import org.apache.flink.runtime.rest.messages.MessageHeaders;
import org.apache.flink.runtime.rest.messages.job.SubtaskExecutionAttemptDetailsInfo;
import org.apache.flink.runtime.rest.messages.job.SubtaskMessageParameters;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;
import org.apache.flink.util.Preconditions;

import java.util.Map;
import java.util.concurrent.Executor;

/**
 * Request handler providing details about a single task execution attempt.
 */
public class SubtaskCurrentAttemptDetailsHandler extends AbstractSubtaskHandler<SubtaskExecutionAttemptDetailsInfo, SubtaskMessageParameters> {

	private final MetricFetcher metricFetcher;

	public SubtaskCurrentAttemptDetailsHandler(
		GatewayRetriever<? extends RestfulGateway> leaderRetriever,
		Time timeout,
		Map<String, String> responseHeaders,
		MessageHeaders<EmptyRequestBody, SubtaskExecutionAttemptDetailsInfo, SubtaskMessageParameters> messageHeaders,
		ExecutionGraphCache executionGraphCache,
		Executor executor,
		MetricFetcher metricFetcher) {

		super(leaderRetriever, timeout, responseHeaders, messageHeaders, executionGraphCache, executor);

		this.metricFetcher = Preconditions.checkNotNull(metricFetcher);
	}

	@Override
	protected SubtaskExecutionAttemptDetailsInfo handleRequest(
			HandlerRequest<EmptyRequestBody, SubtaskMessageParameters> request,
			AccessExecutionVertex executionVertex) throws RestHandlerException {

		final AccessExecution execution = executionVertex.getCurrentExecutionAttempt();

		final JobID jobID = request.getPathParameter(JobIDPathParameter.class);
		final JobVertexID jobVertexID = request.getPathParameter(JobVertexIdPathParameter.class);

		return SubtaskExecutionAttemptDetailsInfo.create(execution, metricFetcher, jobID, jobVertexID);
	}
}
