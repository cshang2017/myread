package org.apache.flink.table.client.gateway.local;

import org.apache.flink.api.dag.Pipeline;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.core.execution.DefaultExecutorServiceLoader;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.execution.PipelineExecutor;
import org.apache.flink.core.execution.PipelineExecutorFactory;
import org.apache.flink.core.execution.PipelineExecutorServiceLoader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;

/**
 * The helper class to deploy a table program on the cluster.
 */
public class ProgramDeployer {

	private final Configuration configuration;
	private final Pipeline pipeline;
	private final String jobName;

	/**
	 * Deploys a table program on the cluster.
	 *
	 * @param configuration  the {@link Configuration} that is used for deployment
	 * @param jobName        job name of the Flink job to be submitted
	 * @param pipeline       Flink {@link Pipeline} to execute
	 */
	public ProgramDeployer(
			Configuration configuration,
			String jobName,
			Pipeline pipeline) {
		this.configuration = configuration;
		this.pipeline = pipeline;
		this.jobName = jobName;
	}

	public CompletableFuture<JobClient> deploy() {

		if (configuration.get(DeploymentOptions.TARGET) == null) {
			throw new RuntimeException("No execution.target specified in your configuration file.");
		}

		PipelineExecutorServiceLoader executorServiceLoader = new DefaultExecutorServiceLoader();
		final PipelineExecutorFactory executorFactory;
		try {
			executorFactory = executorServiceLoader.getExecutorFactory(configuration);
		} catch (Exception e) {
			throw new RuntimeException("Could not retrieve ExecutorFactory.", e);
		}

		final PipelineExecutor executor = executorFactory.getExecutor(configuration);
		CompletableFuture<JobClient> jobClient;
			jobClient = executor.execute(pipeline, configuration);
		return jobClient;
	}
}

