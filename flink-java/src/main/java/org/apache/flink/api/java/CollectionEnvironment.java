

package org.apache.flink.api.java;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.Plan;
import org.apache.flink.api.common.operators.CollectionExecutor;
import org.apache.flink.api.java.utils.CollectionPipelineExecutor;
import org.apache.flink.configuration.DeploymentOptions;

/**
 * Version of {@link ExecutionEnvironment} that allows serial, local, collection-based executions of Flink programs.
 */
@PublicEvolving
public class CollectionEnvironment extends ExecutionEnvironment {

	public CollectionEnvironment() {
		getConfiguration().set(DeploymentOptions.TARGET, CollectionPipelineExecutor.NAME);
		getConfiguration().set(DeploymentOptions.ATTACHED, true);
	}

	@Override
	public JobExecutionResult execute(String jobName) throws Exception {
		Plan p = createProgramPlan(jobName);

		// We need to reverse here. Object-Reuse enabled, means safe mode is disabled.
		CollectionExecutor exec = new CollectionExecutor(getConfig());
		this.lastJobExecutionResult = exec.execute(p);
		return this.lastJobExecutionResult;
	}

	@Override
	public int getParallelism() {
		return 1; // always serial
	}
}
