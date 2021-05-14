package org.apache.flink.table.filesystem.stream;

import org.apache.flink.api.common.state.OperatorStateStore;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;

import java.io.IOException;
import java.util.List;

import static org.apache.flink.table.filesystem.FileSystemOptions.SINK_PARTITION_COMMIT_TRIGGER;

/**
 * Partition commit trigger.
 * See {@link PartitionTimeCommitTigger}.
 * See {@link ProcTimeCommitTigger}.
 */
public interface PartitionCommitTrigger {

	String PARTITION_TIME = "partition-time";
	String PROCESS_TIME = "process-time";

	/**
	 * Add a pending partition.
	 */
	void addPartition(String partition);

	/**
	 * Get committable partitions, and cleanup useless watermarks and partitions.
	 */
	List<String> committablePartitions(long checkpointId) throws IOException;

	/**
	 * End input, return committable partitions and clear.
	 */
	List<String> endInput();

	/**
	 * Snapshot state.
	 */
	void snapshotState(long checkpointId, long watermark) throws Exception;

	static PartitionCommitTrigger create(
			boolean isRestored,
			OperatorStateStore stateStore,
			Configuration conf,
			ClassLoader cl,
			List<String> partitionKeys,
			ProcessingTimeService procTimeService) throws Exception {
		String trigger = conf.get(SINK_PARTITION_COMMIT_TRIGGER);
		switch (trigger) {
			case PARTITION_TIME:
				return new PartitionTimeCommitTigger(
						isRestored, stateStore, conf, cl, partitionKeys);
			case PROCESS_TIME:
				return new ProcTimeCommitTigger(
						isRestored, stateStore, conf, procTimeService);
			default:
				throw new UnsupportedOperationException(
						"Unsupported partition commit trigger: " + trigger);
		}
	}
}
