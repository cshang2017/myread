

package org.apache.flink.runtime.deployment;

import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.io.network.partition.consumer.SingleInputGate;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.shuffle.ShuffleDescriptor;

import javax.annotation.Nonnegative;

import java.io.Serializable;
import java.util.Arrays;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Deployment descriptor for a single input gate instance.
 *
 * <p>Each input gate consumes partitions of a single intermediate result. The consumed
 * subpartition index is the same for each consumed partition.
 *
 * @see SingleInputGate
 */
public class InputGateDeploymentDescriptor implements Serializable {

	/**
	 * The ID of the consumed intermediate result. Each input gate consumes partitions of the
	 * intermediate result specified by this ID. This ID also identifies the input gate at the
	 * consuming task.
	 */
	private final IntermediateDataSetID consumedResultId;

	/** The type of the partition the input gate is going to consume. */
	private final ResultPartitionType consumedPartitionType;

	/**
	 * The index of the consumed subpartition of each consumed partition. This index depends on the
	 * {@link DistributionPattern} and the subtask indices of the producing and consuming task.
	 */
	@Nonnegative
	private final int consumedSubpartitionIndex;

	/** An input channel for each consumed subpartition. */
	private final ShuffleDescriptor[] inputChannels;

	public InputGateDeploymentDescriptor(
			IntermediateDataSetID consumedResultId,
			ResultPartitionType consumedPartitionType,
			@Nonnegative int consumedSubpartitionIndex,
			ShuffleDescriptor[] inputChannels) {
		this.consumedResultId = checkNotNull(consumedResultId);
		this.consumedPartitionType = checkNotNull(consumedPartitionType);
		this.consumedSubpartitionIndex = consumedSubpartitionIndex;
		this.inputChannels = checkNotNull(inputChannels);
	}

	public IntermediateDataSetID getConsumedResultId() {
		return consumedResultId;
	}

	/**
	 * Returns the type of this input channel's consumed result partition.
	 *
	 * @return consumed result partition type
	 */
	public ResultPartitionType getConsumedPartitionType() {
		return consumedPartitionType;
	}

	@Nonnegative
	public int getConsumedSubpartitionIndex() {
		return consumedSubpartitionIndex;
	}

	public ShuffleDescriptor[] getShuffleDescriptors() {
		return inputChannels;
	}

}
