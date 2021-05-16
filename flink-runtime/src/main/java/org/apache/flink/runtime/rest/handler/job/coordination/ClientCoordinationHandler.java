package org.apache.flink.runtime.rest.handler.job.coordination;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.coordination.CoordinationRequest;
import org.apache.flink.runtime.operators.coordination.CoordinationResponse;
import org.apache.flink.runtime.rest.handler.AbstractRestHandler;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.rest.messages.JobIDPathParameter;
import org.apache.flink.runtime.rest.messages.MessageHeaders;
import org.apache.flink.runtime.rest.messages.OperatorIDPathParameter;
import org.apache.flink.runtime.rest.messages.job.coordination.ClientCoordinationMessageParameters;
import org.apache.flink.runtime.rest.messages.job.coordination.ClientCoordinationRequestBody;
import org.apache.flink.runtime.rest.messages.job.coordination.ClientCoordinationResponseBody;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;
import org.apache.flink.util.SerializedValue;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

/**
 * Handler that receives the coordination requests from the client and returns the response from the coordinator.
 */
public class ClientCoordinationHandler extends AbstractRestHandler<RestfulGateway, ClientCoordinationRequestBody, ClientCoordinationResponseBody, ClientCoordinationMessageParameters> {

	public ClientCoordinationHandler(
			GatewayRetriever<? extends RestfulGateway> leaderRetriever,
			Time timeout,
			Map<String, String> responseHeaders,
			MessageHeaders<ClientCoordinationRequestBody, ClientCoordinationResponseBody, ClientCoordinationMessageParameters> messageHeaders) {
		super(leaderRetriever, timeout, responseHeaders, messageHeaders);
	}

	@Override
	protected CompletableFuture<ClientCoordinationResponseBody> handleRequest(
			@Nonnull HandlerRequest<ClientCoordinationRequestBody, ClientCoordinationMessageParameters> request,
			@Nonnull RestfulGateway gateway) throws RestHandlerException {
		JobID jobId = request.getPathParameter(JobIDPathParameter.class);
		OperatorID operatorId = request.getPathParameter(OperatorIDPathParameter.class);
		SerializedValue<CoordinationRequest> serializedRequest =
			request.getRequestBody().getSerializedCoordinationRequest();
		CompletableFuture<CoordinationResponse> responseFuture =
			gateway.deliverCoordinationRequestToCoordinator(jobId, operatorId, serializedRequest, timeout);
		return responseFuture.thenApply(
			coordinationResponse -> {
					return new ClientCoordinationResponseBody(new SerializedValue<>(coordinationResponse));
				
			});
	}
}
