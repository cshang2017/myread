package org.apache.flink.client.program;

import org.apache.flink.api.dag.Pipeline;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.ExecutionEnvironmentFactory;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.execution.JobClient;

/**
 * An {@link ExecutionEnvironment} that never executes a job but only extracts the {@link Pipeline}.
 */
public class OptimizerPlanEnvironment extends ExecutionEnvironment {

	private Pipeline pipeline;

	public Pipeline getPipeline() {
		return pipeline;
	}

	public OptimizerPlanEnvironment(Configuration configuration, ClassLoader userClassloader, int parallelism) {
		super(configuration, userClassloader);
		if (parallelism > 0) {
			setParallelism(parallelism);
		}
	}

	@Override
	public JobClient executeAsync(String jobName) {
		pipeline = createProgramPlan();
		

	}

	public void setAsContext() {
		ExecutionEnvironmentFactory factory = () -> this;
		initializeContextEnvironment(factory);
	}

	public void unsetAsContext() {
		resetContextEnvironment();
	}

}
