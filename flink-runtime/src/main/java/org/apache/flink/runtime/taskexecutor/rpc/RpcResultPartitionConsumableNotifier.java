
package org.apache.flink.runtime.taskexecutor.rpc;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.io.network.partition.ResultPartitionConsumableNotifier;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.jobmaster.JobMasterGateway;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.taskmanager.TaskActions;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

public class RpcResultPartitionConsumableNotifier implements ResultPartitionConsumableNotifier {

	private static final Logger LOG = LoggerFactory.getLogger(RpcResultPartitionConsumableNotifier.class);

	private final JobMasterGateway jobMasterGateway;
	private final Executor executor;
	private final Time timeout;

	public RpcResultPartitionConsumableNotifier(
			JobMasterGateway jobMasterGateway,
			Executor executor,
			Time timeout) {
		this.jobMasterGateway = Preconditions.checkNotNull(jobMasterGateway);
		this.executor = Preconditions.checkNotNull(executor);
		this.timeout = Preconditions.checkNotNull(timeout);
	}
	@Override
	public void notifyPartitionConsumable(JobID jobId, ResultPartitionID partitionId, final TaskActions taskActions) {
		CompletableFuture<Acknowledge> acknowledgeFuture = jobMasterGateway.scheduleOrUpdateConsumers(partitionId, timeout);

		acknowledgeFuture.whenCompleteAsync(
			(Acknowledge ack, Throwable throwable) -> {
				if (throwable != null) {
					LOG.error("Could not schedule or update consumers at the JobManager.", throwable);

					taskActions.failExternally(new RuntimeException("Could not notify JobManager to schedule or update consumers.", throwable));
				}
			},
			executor);
	}
}
