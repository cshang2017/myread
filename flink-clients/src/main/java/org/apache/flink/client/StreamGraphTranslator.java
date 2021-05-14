package org.apache.flink.client;

import org.apache.flink.api.dag.Pipeline;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.streaming.api.graph.StreamGraph;


import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * {@link FlinkPipelineTranslator} for DataStream API {@link StreamGraph StreamGraphs}.
 *
 * <p>Note: this is used through reflection in
 * {@link org.apache.flink.client.FlinkPipelineTranslationUtil}.
 */
@SuppressWarnings("unused")
public class StreamGraphTranslator implements FlinkPipelineTranslator {

	@Override
	public JobGraph translateToJobGraph(
			Pipeline pipeline,
			Configuration optimizerConfiguration,
			int defaultParallelism) {

		StreamGraph streamGraph = (StreamGraph) pipeline;
		return streamGraph.getJobGraph(null);
	}

	@Override
	public String translateToJSONExecutionPlan(Pipeline pipeline) {

		StreamGraph streamGraph = (StreamGraph) pipeline;
		return streamGraph.getStreamingPlanAsJSON();
	}

	@Override
	public boolean canTranslate(Pipeline pipeline) {
		return pipeline instanceof StreamGraph;
	}
}
