package org.apache.flink.runtime.rest.handler.job;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.accumulators.StringifiedAccumulatorResult;
import org.apache.flink.runtime.executiongraph.AccessExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.AccessExecutionVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.rest.handler.legacy.ExecutionGraphCache;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.JobVertexMessageParameters;
import org.apache.flink.runtime.rest.messages.MessageHeaders;
import org.apache.flink.runtime.rest.messages.job.SubtasksAllAccumulatorsInfo;
import org.apache.flink.runtime.rest.messages.job.UserAccumulator;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;

/**
 * Request handler for the subtasks all accumulators.
 */
public class SubtasksAllAccumulatorsHandler extends AbstractJobVertexHandler<SubtasksAllAccumulatorsInfo, JobVertexMessageParameters> {

	public SubtasksAllAccumulatorsHandler(GatewayRetriever<? extends RestfulGateway> leaderRetriever, Time timeout, Map<String, String> responseHeaders, MessageHeaders<EmptyRequestBody, SubtasksAllAccumulatorsInfo, JobVertexMessageParameters> messageHeaders, ExecutionGraphCache executionGraphCache, Executor executor) {
		super(leaderRetriever, timeout, responseHeaders, messageHeaders, executionGraphCache, executor);
	}

	@Override
	protected SubtasksAllAccumulatorsInfo handleRequest(HandlerRequest<EmptyRequestBody, JobVertexMessageParameters> request, AccessExecutionJobVertex jobVertex) throws RestHandlerException {
		JobVertexID jobVertexId = jobVertex.getJobVertexId();
		int parallelism = jobVertex.getParallelism();

		final List<SubtasksAllAccumulatorsInfo.SubtaskAccumulatorsInfo> subtaskAccumulatorsInfos = new ArrayList<>();

		for (AccessExecutionVertex vertex : jobVertex.getTaskVertices()) {
			TaskManagerLocation location = vertex.getCurrentAssignedResourceLocation();
			String locationString = location == null ? "(unassigned)" : location.getHostname();

			StringifiedAccumulatorResult[] accs = vertex.getCurrentExecutionAttempt().getUserAccumulatorsStringified();
			List<UserAccumulator> userAccumulators = new ArrayList<>(accs.length);
			for (StringifiedAccumulatorResult acc : accs) {
				userAccumulators.add(new UserAccumulator(acc.getName(), acc.getType(), acc.getValue()));
			}

			subtaskAccumulatorsInfos.add(
				new SubtasksAllAccumulatorsInfo.SubtaskAccumulatorsInfo(
					vertex.getCurrentExecutionAttempt().getParallelSubtaskIndex(),
					vertex.getCurrentExecutionAttempt().getAttemptNumber(),
					locationString,
					userAccumulators
				));
		}

		return new SubtasksAllAccumulatorsInfo(jobVertexId, parallelism, subtaskAccumulatorsInfos);
	}

}
