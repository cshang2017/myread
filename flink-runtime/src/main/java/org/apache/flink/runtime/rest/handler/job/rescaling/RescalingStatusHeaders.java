package org.apache.flink.runtime.rest.handler.job.rescaling;

import org.apache.flink.runtime.rest.HttpMethodWrapper;
import org.apache.flink.runtime.rest.handler.async.AsynchronousOperationInfo;
import org.apache.flink.runtime.rest.handler.async.AsynchronousOperationStatusMessageHeaders;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.JobIDPathParameter;
import org.apache.flink.runtime.rest.messages.TriggerIdPathParameter;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

/**
 * Message headers for polling the status of an ongoing rescaling operation.
 */
public class RescalingStatusHeaders extends
	AsynchronousOperationStatusMessageHeaders<AsynchronousOperationInfo, RescalingStatusMessageParameters> {

	private static final RescalingStatusHeaders INSTANCE = new RescalingStatusHeaders();

	private static final String URL = String.format(
		"/jobs/:%s/rescaling/:%s",
		JobIDPathParameter.KEY,
		TriggerIdPathParameter.KEY);

	private RescalingStatusHeaders() {}

	@Override
	public Class<AsynchronousOperationInfo> getValueClass() {
		return AsynchronousOperationInfo.class;
	}

	@Override
	public Class<EmptyRequestBody> getRequestClass() {
		return EmptyRequestBody.class;
	}

	@Override
	public HttpResponseStatus getResponseStatusCode() {
		return HttpResponseStatus.OK;
	}

	@Override
	public RescalingStatusMessageParameters getUnresolvedMessageParameters() {
		return new RescalingStatusMessageParameters();
	}

	@Override
	public HttpMethodWrapper getHttpMethod() {
		return HttpMethodWrapper.GET;
	}

	@Override
	public String getTargetRestEndpointURL() {
		return URL;
	}

	public static RescalingStatusHeaders getInstance() {
		return INSTANCE;
	}

	@Override
	public String getDescription() {
		return "Returns the status of a rescaling operation.";
	}
}
