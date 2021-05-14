package org.apache.flink.client.deployment.executors;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.dag.Pipeline;
import org.apache.flink.client.FlinkPipelineTranslationUtil;
import org.apache.flink.client.cli.ExecutionConfigAccessor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.PipelineOptionsInternal;
import org.apache.flink.runtime.jobgraph.JobGraph;

import javax.annotation.Nonnull;

import java.net.MalformedURLException;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Utility class with method related to job execution.
 */
public class PipelineExecutorUtils {

	/**
	 * Creates the {@link JobGraph} corresponding to the provided {@link Pipeline}.
	 *
	 * @param pipeline the pipeline whose job graph we are computing
	 * @param configuration the configuration with the necessary information such as jars and
	 *                         classpaths to be included, the parallelism of the job and potential
	 *                         savepoint settings used to bootstrap its state.
	 * @return the corresponding {@link JobGraph}.
	 */
	public static JobGraph getJobGraph(Pipeline pipeline, Configuration configuration) {

		ExecutionConfigAccessor executionConfigAccessor = ExecutionConfigAccessor.fromConfiguration(configuration);
		JobGraph jobGraph = FlinkPipelineTranslationUtil
				.getJobGraph(pipeline, configuration, executionConfigAccessor.getParallelism());

		configuration
				.getOptional(PipelineOptionsInternal.PIPELINE_FIXED_JOB_ID)
				.ifPresent(strJobID -> jobGraph.setJobID(JobID.fromHexString(strJobID)));

		jobGraph.addJars(executionConfigAccessor.getJars());
		jobGraph.setClasspaths(executionConfigAccessor.getClasspaths());
		jobGraph.setSavepointRestoreSettings(executionConfigAccessor.getSavepointRestoreSettings());

		return jobGraph;
	}
}
