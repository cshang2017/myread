package org.apache.flink.table.planner.utils;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.InputDependencyConstraint;
import org.apache.flink.api.common.operators.ResourceSpec;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.runtime.jobgraph.ScheduleMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.graph.GlobalDataExchangeMode;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.graph.StreamGraphGenerator;
import org.apache.flink.table.api.TableConfig;

import java.util.List;

/**
 * Utility class to generate StreamGraph and set properties for batch.
 */
public class ExecutorUtils {

	/**
	 * Generate {@link StreamGraph} by {@link StreamGraphGenerator}.
	 */
	public static StreamGraph generateStreamGraph(
			StreamExecutionEnvironment execEnv,
			List<Transformation<?>> transformations) {
	
		StreamGraphGenerator generator =
				new StreamGraphGenerator(transformations, execEnv.getConfig(), execEnv.getCheckpointConfig())
						.setStateBackend(execEnv.getStateBackend())
						.setChaining(execEnv.isChainingEnabled())
						.setUserArtifacts(execEnv.getCachedFiles())
						.setTimeCharacteristic(execEnv.getStreamTimeCharacteristic())
						.setDefaultBufferTimeout(execEnv.getBufferTimeout());
		return generator.generate();
	}

	/**
	 * Sets batch properties for {@link StreamExecutionEnvironment}.
	 */
	public static void setBatchProperties(StreamExecutionEnvironment execEnv, TableConfig tableConfig) {
		ExecutionConfig executionConfig = execEnv.getConfig();
		executionConfig.enableObjectReuse();
		executionConfig.setLatencyTrackingInterval(-1);
		execEnv.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
		execEnv.setBufferTimeout(-1);
		if (isShuffleModeAllBlocking(tableConfig)) {
			executionConfig.setDefaultInputDependencyConstraint(InputDependencyConstraint.ALL);
		}
	}

	/**
	 * Sets batch properties for {@link StreamGraph}.
	 */
	public static void setBatchProperties(StreamGraph streamGraph, TableConfig tableConfig) {
		streamGraph.getStreamNodes().forEach(
				sn -> sn.setResources(ResourceSpec.UNKNOWN, ResourceSpec.UNKNOWN));
		streamGraph.setChaining(true);
		streamGraph.setAllVerticesInSameSlotSharingGroupByDefault(false);
		streamGraph.setScheduleMode(ScheduleMode.LAZY_FROM_SOURCES_WITH_BATCH_SLOT_REQUEST);
		streamGraph.setStateBackend(null);
		if (streamGraph.getCheckpointConfig().isCheckpointingEnabled()) {
			throw new IllegalArgumentException("Checkpoint is not supported for batch jobs.");
		}
		streamGraph.setGlobalDataExchangeMode(getGlobalDataExchangeMode(tableConfig));
	}

	private static boolean isShuffleModeAllBlocking(TableConfig tableConfig) {
		return getGlobalDataExchangeMode(tableConfig) == GlobalDataExchangeMode.ALL_EDGES_BLOCKING;
	}

	private static GlobalDataExchangeMode getGlobalDataExchangeMode(TableConfig tableConfig) {
		return ShuffleModeUtils.getShuffleModeAsGlobalDataExchangeMode(tableConfig.getConfiguration());
	}
}
