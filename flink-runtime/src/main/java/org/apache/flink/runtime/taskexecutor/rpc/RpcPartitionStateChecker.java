package org.apache.flink.runtime.taskexecutor.rpc;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.taskexecutor.PartitionProducerStateChecker;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobmaster.JobMasterGateway;
import org.apache.flink.util.Preconditions;

import java.util.concurrent.CompletableFuture;

public class RpcPartitionStateChecker implements PartitionProducerStateChecker {

	private final JobMasterGateway jobMasterGateway;

	public RpcPartitionStateChecker(JobMasterGateway jobMasterGateway) {
		this.jobMasterGateway = Preconditions.checkNotNull(jobMasterGateway);
	}

	@Override
	public CompletableFuture<ExecutionState> requestPartitionProducerState(
			JobID jobId,
			IntermediateDataSetID resultId,
			ResultPartitionID partitionId) {

		return jobMasterGateway.requestPartitionState(resultId, partitionId);
	}
}
