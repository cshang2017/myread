

package org.apache.flink.runtime.rest.handler.job.rescaling;

import org.apache.flink.runtime.rest.messages.JobMessageParameters;
import org.apache.flink.runtime.rest.messages.MessageParameters;
import org.apache.flink.runtime.rest.messages.MessageQueryParameter;
import org.apache.flink.runtime.rest.messages.RescalingParallelismQueryParameter;

import java.util.Collection;
import java.util.Collections;

/**
 * {@link MessageParameters} for triggering the rescaling of a job.
 */
public class RescalingTriggerMessageParameters extends JobMessageParameters {

	public final RescalingParallelismQueryParameter rescalingParallelismQueryParameter = new RescalingParallelismQueryParameter();

	@Override
	public Collection<MessageQueryParameter<?>> getQueryParameters() {
		return Collections.singleton(rescalingParallelismQueryParameter);
	}
}
