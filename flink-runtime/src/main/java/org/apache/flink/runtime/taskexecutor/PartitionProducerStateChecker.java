package org.apache.flink.runtime.taskexecutor;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;

import java.util.concurrent.CompletableFuture;

/**
 * Intermediate partition state checker to query the JobManager about the state
 * of the producer of a result partition.
 *
 * <p>These checks are triggered when a partition request is answered with a
 * PartitionNotFound event. This usually happens when the producer of that
 * partition has not registered itself with the network stack or terminated.
 */
public interface PartitionProducerStateChecker {

	/**
	 * Requests the execution state of the execution producing a result partition.
	 *
	 * @param jobId ID of the job the partition belongs to.
	 * @param intermediateDataSetId ID of the parent intermediate data set.
	 * @param resultPartitionId ID of the result partition to check. This
	 * identifies the producing execution and partition.
	 *
	 * @return Future holding the execution state of the producing execution.
	 */
	CompletableFuture<ExecutionState> requestPartitionProducerState(
			JobID jobId,
			IntermediateDataSetID intermediateDataSetId,
			ResultPartitionID resultPartitionId);

}
