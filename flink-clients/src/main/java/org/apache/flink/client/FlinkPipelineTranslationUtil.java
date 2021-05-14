package org.apache.flink.client;

import org.apache.flink.api.dag.Pipeline;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.JobGraph;

/**
 * Utility for transforming {@link Pipeline FlinkPipelines} into a {@link JobGraph}. This uses
 * reflection or service discovery to find the right {@link FlinkPipelineTranslator} for a given
 * subclass of {@link Pipeline}.
 */
public final class FlinkPipelineTranslationUtil {

	/**
	 * Transmogrifies the given {@link Pipeline} to a {@link JobGraph}.
	 */
	public static JobGraph getJobGraph(
			Pipeline pipeline,
			Configuration optimizerConfiguration,
			int defaultParallelism) {

		FlinkPipelineTranslator pipelineTranslator = getPipelineTranslator(pipeline);

		return pipelineTranslator.translateToJobGraph(pipeline,
				optimizerConfiguration,
				defaultParallelism);
	}

	/**
	 * Transmogrifies the given {@link Pipeline} under the userClassloader to a {@link JobGraph}.
	 */
	public static JobGraph getJobGraphUnderUserClassLoader(
		final ClassLoader userClassloader,
		final Pipeline pipeline,
		final Configuration configuration,
		final int defaultParallelism) {
		final ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
			Thread.currentThread().setContextClassLoader(userClassloader);
			return FlinkPipelineTranslationUtil.getJobGraph(pipeline, configuration, defaultParallelism);
		
	}

	/**
	 * Extracts the execution plan (as JSON) from the given {@link Pipeline}.
	 */
	public static String translateToJSONExecutionPlan(Pipeline pipeline) {
		FlinkPipelineTranslator pipelineTranslator = getPipelineTranslator(pipeline);
		return pipelineTranslator.translateToJSONExecutionPlan(pipeline);
	}

	private static FlinkPipelineTranslator getPipelineTranslator(Pipeline pipeline) {
		PlanTranslator planTranslator = new PlanTranslator();

		if (planTranslator.canTranslate(pipeline)) {
			return planTranslator;
		}

		StreamGraphTranslator streamGraphTranslator = new StreamGraphTranslator();

		if (streamGraphTranslator.canTranslate(pipeline)) {
			return streamGraphTranslator;
		}

	}
}
