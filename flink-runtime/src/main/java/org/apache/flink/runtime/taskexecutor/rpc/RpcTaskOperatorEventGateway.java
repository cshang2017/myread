package org.apache.flink.runtime.taskexecutor.rpc;

import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobgraph.tasks.TaskOperatorEventGateway;
import org.apache.flink.runtime.jobmaster.JobMasterOperatorEventGateway;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.util.SerializedValue;

import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

/**
 * An OperatorEventSender that calls the RPC gateway {@link JobMasterOperatorEventGateway} to
 * send the messages to the coordinator.
 */
public class RpcTaskOperatorEventGateway implements TaskOperatorEventGateway {

	private final JobMasterOperatorEventGateway rpcGateway;

	private final ExecutionAttemptID taskExecutionId;

	private final Consumer<Throwable> errorHandler;

	public RpcTaskOperatorEventGateway(
			JobMasterOperatorEventGateway rpcGateway,
			ExecutionAttemptID taskExecutionId,
			Consumer<Throwable> errorHandler) {

		this.rpcGateway = rpcGateway;
		this.taskExecutionId = taskExecutionId;
		this.errorHandler = errorHandler;
	}

	@Override
	public void sendOperatorEventToCoordinator(OperatorID operator, SerializedValue<OperatorEvent> event) {
		final CompletableFuture<Acknowledge> result =
			rpcGateway.sendOperatorEventToCoordinator(taskExecutionId, operator, event);

		result.whenComplete((success, exception) -> {
			if (exception != null) {
				errorHandler.accept(exception);
			}
		});
	}
}
