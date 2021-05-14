
package org.apache.flink.client.program;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.dag.Pipeline;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironmentFactory;
import org.apache.flink.streaming.api.graph.StreamGraph;

/**
 * A special {@link StreamExecutionEnvironment} that is used in the web frontend when generating
 * a user-inspectable graph of a streaming job.
 */
@PublicEvolving
public class StreamPlanEnvironment extends StreamExecutionEnvironment {

	private Pipeline pipeline;

	public Pipeline getPipeline() {
		return pipeline;
	}

	public StreamPlanEnvironment(Configuration configuration, ClassLoader userClassLoader, int parallelism) {
		super(configuration, userClassLoader);
		if (parallelism > 0) {
			setParallelism(parallelism);
		}
	}

	@Override
	public JobClient executeAsync(StreamGraph streamGraph) {
		pipeline = streamGraph;

		// do not go on with anything now!
		throw new ProgramAbortException();
	}

	public void setAsContext() {
		StreamExecutionEnvironmentFactory factory = () -> this;
		initializeContextEnvironment(factory);
	}

	public void unsetAsContext() {
		resetContextEnvironment();
	}
}
