package org.apache.flink.runtime.rest.handler.job.rescaling;

import org.apache.flink.runtime.rest.messages.JobMessageParameters;
import org.apache.flink.runtime.rest.messages.MessageParameters;
import org.apache.flink.runtime.rest.messages.MessagePathParameter;
import org.apache.flink.runtime.rest.messages.TriggerIdPathParameter;

import java.util.Arrays;
import java.util.Collection;

/**
 * {@link MessageParameters} for polling the status of a rescaling operation.
 */
public class RescalingStatusMessageParameters extends JobMessageParameters {

	public final TriggerIdPathParameter triggerIdPathParameter = new TriggerIdPathParameter();

	@Override
	public Collection<MessagePathParameter<?>> getPathParameters() {
		return Arrays.asList(jobPathParameter, triggerIdPathParameter);
	}
}
