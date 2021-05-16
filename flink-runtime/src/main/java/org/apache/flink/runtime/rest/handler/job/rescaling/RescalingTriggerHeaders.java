
package org.apache.flink.runtime.rest.handler.job.rescaling;

import org.apache.flink.runtime.rest.HttpMethodWrapper;
import org.apache.flink.runtime.rest.handler.async.AsynchronousOperationTriggerMessageHeaders;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.JobIDPathParameter;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

/**
 * Message headers for triggering the rescaling of a job.
 */
public class RescalingTriggerHeaders extends
	AsynchronousOperationTriggerMessageHeaders<EmptyRequestBody, RescalingTriggerMessageParameters> {

	private static final RescalingTriggerHeaders INSTANCE = new RescalingTriggerHeaders();

	private static final String URL = String.format(
		"/jobs/:%s/rescaling",
		JobIDPathParameter.KEY);

	private RescalingTriggerHeaders() {}

	@Override
	public Class<EmptyRequestBody> getRequestClass() {
		return EmptyRequestBody.class;
	}

	@Override
	public HttpResponseStatus getResponseStatusCode() {
		return HttpResponseStatus.OK;
	}

	@Override
	public RescalingTriggerMessageParameters getUnresolvedMessageParameters() {
		return new RescalingTriggerMessageParameters();
	}

	@Override
	public HttpMethodWrapper getHttpMethod() {
		return HttpMethodWrapper.PATCH;
	}

	@Override
	public String getTargetRestEndpointURL() {
		return URL;
	}

	public static RescalingTriggerHeaders getInstance() {
		return INSTANCE;
	}

	@Override
	protected String getAsyncOperationDescription() {
		return "Triggers the rescaling of a job.";
	}
}
