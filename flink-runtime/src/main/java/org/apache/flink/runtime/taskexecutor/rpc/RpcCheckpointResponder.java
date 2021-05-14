package org.apache.flink.runtime.taskexecutor.rpc;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.checkpoint.CheckpointCoordinatorGateway;
import org.apache.flink.runtime.checkpoint.CheckpointMetrics;
import org.apache.flink.runtime.checkpoint.TaskStateSnapshot;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.messages.checkpoint.DeclineCheckpoint;
import org.apache.flink.runtime.taskmanager.CheckpointResponder;
import org.apache.flink.util.Preconditions;

public class RpcCheckpointResponder implements CheckpointResponder {

	private final CheckpointCoordinatorGateway checkpointCoordinatorGateway;

	public RpcCheckpointResponder(CheckpointCoordinatorGateway checkpointCoordinatorGateway) {
		this.checkpointCoordinatorGateway = Preconditions.checkNotNull(checkpointCoordinatorGateway);
	}

	@Override
	public void acknowledgeCheckpoint(
			JobID jobID,
			ExecutionAttemptID executionAttemptID,
			long checkpointId,
			CheckpointMetrics checkpointMetrics,
			TaskStateSnapshot subtaskState) {

		checkpointCoordinatorGateway.acknowledgeCheckpoint(
			jobID,
			executionAttemptID,
			checkpointId,
			checkpointMetrics,
			subtaskState);
	}

	@Override
	public void declineCheckpoint(
			JobID jobID,
			ExecutionAttemptID executionAttemptID,
			long checkpointId,
			Throwable cause) {

		checkpointCoordinatorGateway.declineCheckpoint(new DeclineCheckpoint(jobID,
			executionAttemptID,
			checkpointId,
			cause));
	}
}
