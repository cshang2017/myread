package org.apache.flink.client;

import org.apache.flink.api.dag.Pipeline;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.JobGraph;

/**
 * This can be used to turn a {@link Pipeline} into a {@link JobGraph}. There will be
 * implementations for the different pipeline APIs that Flink supports.
 */
public interface FlinkPipelineTranslator {

	/**
	 * Creates a {@link JobGraph} from the given {@link Pipeline} and attaches the given jar
	 * files and classpaths to the {@link JobGraph}.
	 */
	JobGraph translateToJobGraph(
			Pipeline pipeline,
			Configuration optimizerConfiguration,
			int defaultParallelism);


	/**
	 * Extracts the execution plan (as JSON) from the given {@link Pipeline}.
	 */
	String translateToJSONExecutionPlan(Pipeline pipeline);

	boolean canTranslate(Pipeline pipeline);
}
