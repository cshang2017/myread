package org.apache.flink.runtime.rest.handler.job;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.accumulators.StringifiedAccumulatorResult;
import org.apache.flink.runtime.executiongraph.AccessExecutionJobVertex;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.rest.handler.legacy.ExecutionGraphCache;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.JobVertexAccumulatorsInfo;
import org.apache.flink.runtime.rest.messages.JobVertexMessageParameters;
import org.apache.flink.runtime.rest.messages.MessageHeaders;
import org.apache.flink.runtime.rest.messages.job.UserAccumulator;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;

import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.Executor;

/**
 * Request handler for the job vertex accumulators.
 */
public class JobVertexAccumulatorsHandler extends AbstractJobVertexHandler<JobVertexAccumulatorsInfo, JobVertexMessageParameters> {

	public JobVertexAccumulatorsHandler(
			GatewayRetriever<? extends RestfulGateway> leaderRetriever,
			Time timeout,
			Map<String, String> responseHeaders,
			MessageHeaders<EmptyRequestBody, JobVertexAccumulatorsInfo, JobVertexMessageParameters> messageHeaders,
			ExecutionGraphCache executionGraphCache,
			Executor executor) {
		super(
			leaderRetriever,
			timeout,
			responseHeaders,
			messageHeaders,
			executionGraphCache,
			executor);
	}

	@Override
	protected JobVertexAccumulatorsInfo handleRequest(
			HandlerRequest<EmptyRequestBody, JobVertexMessageParameters> request,
			AccessExecutionJobVertex jobVertex) throws RestHandlerException {

		StringifiedAccumulatorResult[] accs = jobVertex.getAggregatedUserAccumulatorsStringified();
		ArrayList<UserAccumulator> userAccumulatorList = new ArrayList<>(accs.length);

		for (StringifiedAccumulatorResult acc : accs) {
			userAccumulatorList.add(
				new UserAccumulator(
					acc.getName(),
					acc.getType(),
					acc.getValue()));
		}

		return new JobVertexAccumulatorsInfo(jobVertex.getJobVertexId().toString(), userAccumulatorList);
	}
}
