package org.apache.flink.table.executor;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.dag.Pipeline;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.graph.StreamGraphGenerator;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.delegation.Executor;

import java.util.List;

/**
 * An implementation of {@link Executor} that is backed by a {@link StreamExecutionEnvironment}.
 * This is the only executor that {@link org.apache.flink.table.planner.StreamPlanner} supports.
 */
@Internal
public class StreamExecutor implements Executor {
	private final StreamExecutionEnvironment executionEnvironment;

	@VisibleForTesting
	public StreamExecutor(StreamExecutionEnvironment executionEnvironment) {
		this.executionEnvironment = executionEnvironment;
	}

	@Override
	public Pipeline createPipeline(List<Transformation<?>> transformations, TableConfig tableConfig, String jobName) {
		
		return new StreamGraphGenerator(
			transformations, executionEnvironment.getConfig(), executionEnvironment.getCheckpointConfig())
			.setStateBackend(executionEnvironment.getStateBackend())
			.setChaining(executionEnvironment.isChainingEnabled())
			.setUserArtifacts(executionEnvironment.getCachedFiles())
			.setTimeCharacteristic(executionEnvironment.getStreamTimeCharacteristic())
			.setDefaultBufferTimeout(executionEnvironment.getBufferTimeout())
			.setJobName(jobName)
			.generate();
	}

	@Override
	public JobExecutionResult execute(Pipeline pipeline) throws Exception {
		return executionEnvironment.execute((StreamGraph) pipeline);
	}

	@Override
	public JobClient executeAsync(Pipeline pipeline) throws Exception {
		return executionEnvironment.executeAsync((StreamGraph) pipeline);
	}

	public StreamExecutionEnvironment getExecutionEnvironment() {
		return executionEnvironment;
	}
}
