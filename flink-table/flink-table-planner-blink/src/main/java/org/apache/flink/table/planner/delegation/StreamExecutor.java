
package org.apache.flink.table.planner.delegation;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.dag.Pipeline;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.delegation.Executor;
import org.apache.flink.table.planner.utils.ExecutorUtils;

import java.util.List;

/**
 * An implementation of {@link Executor} that is backed by a {@link StreamExecutionEnvironment}.
 * This is the only executor that {@link org.apache.flink.table.planner.delegation.StreamPlanner} supports.
 */
@Internal
public class StreamExecutor extends ExecutorBase {

	@VisibleForTesting
	public StreamExecutor(StreamExecutionEnvironment executionEnvironment) {
		super(executionEnvironment);
	}

	@Override
	public Pipeline createPipeline(List<Transformation<?>> transformations, TableConfig tableConfig, String jobName) {
		StreamGraph streamGraph = ExecutorUtils.generateStreamGraph(getExecutionEnvironment(), transformations);
		streamGraph.setJobName(getNonEmptyJobName(jobName));
		return streamGraph;
	}
}
