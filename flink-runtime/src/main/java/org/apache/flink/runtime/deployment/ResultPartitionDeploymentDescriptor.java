
package org.apache.flink.runtime.deployment;

import org.apache.flink.runtime.io.network.partition.ResultPartition;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.shuffle.PartitionDescriptor;
import org.apache.flink.runtime.shuffle.ShuffleDescriptor;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;

import java.io.Serializable;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Deployment descriptor for a result partition.
 *
 * @see ResultPartition
 */
public class ResultPartitionDeploymentDescriptor implements Serializable {

	private final PartitionDescriptor partitionDescriptor;

	private final ShuffleDescriptor shuffleDescriptor;

	private final int maxParallelism;

	/** Flag whether the result partition should send scheduleOrUpdateConsumer messages. */
	private final boolean sendScheduleOrUpdateConsumersMessage;

	public ResultPartitionDeploymentDescriptor(
			PartitionDescriptor partitionDescriptor,
			ShuffleDescriptor shuffleDescriptor,
			int maxParallelism,
			boolean sendScheduleOrUpdateConsumersMessage) {
		this.partitionDescriptor = checkNotNull(partitionDescriptor);
		this.shuffleDescriptor = checkNotNull(shuffleDescriptor);
		KeyGroupRangeAssignment.checkParallelismPreconditions(maxParallelism);
		this.maxParallelism = maxParallelism;
		this.sendScheduleOrUpdateConsumersMessage = sendScheduleOrUpdateConsumersMessage;
	}

	public IntermediateDataSetID getResultId() {
		return partitionDescriptor.getResultId();
	}

	public IntermediateResultPartitionID getPartitionId() {
		return partitionDescriptor.getPartitionId();
	}

	public ResultPartitionType getPartitionType() {
		return partitionDescriptor.getPartitionType();
	}

	public int getTotalNumberOfPartitions() {
		return partitionDescriptor.getTotalNumberOfPartitions();
	}

	public int getNumberOfSubpartitions() {
		return partitionDescriptor.getNumberOfSubpartitions();
	}

	public int getMaxParallelism() {
		return maxParallelism;
	}

	public ShuffleDescriptor getShuffleDescriptor() {
		return shuffleDescriptor;
	}

	public boolean sendScheduleOrUpdateConsumersMessage() {
		return sendScheduleOrUpdateConsumersMessage;
	}

}
