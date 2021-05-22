package org.apache.flink.runtime.executiongraph;

import org.apache.flink.runtime.executiongraph.failover.flip1.ResultPartitionAvailabilityChecker;
import org.apache.flink.runtime.io.network.partition.JobMasterPartitionTracker;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;

import java.util.function.Function;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A {@link ResultPartitionAvailabilityChecker} which decides the intermediate result partition availability
 * based on whether the corresponding result partition in the execution graph is tracked.
 */
public class ExecutionGraphResultPartitionAvailabilityChecker implements ResultPartitionAvailabilityChecker {

	/** The function maps an IntermediateResultPartitionID to a ResultPartitionID. */
	private final Function<IntermediateResultPartitionID, ResultPartitionID> partitionIDMapper;

	/** The tracker that tracks all available result partitions. */
	private final JobMasterPartitionTracker partitionTracker;

	ExecutionGraphResultPartitionAvailabilityChecker(
			final Function<IntermediateResultPartitionID, ResultPartitionID> partitionIDMapper,
			final JobMasterPartitionTracker partitionTracker) {

		this.partitionIDMapper = checkNotNull(partitionIDMapper);
		this.partitionTracker = checkNotNull(partitionTracker);
	}

	@Override
	public boolean isAvailable(final IntermediateResultPartitionID resultPartitionID) {
		return partitionTracker.isPartitionTracked(partitionIDMapper.apply(resultPartitionID));
	}
}
