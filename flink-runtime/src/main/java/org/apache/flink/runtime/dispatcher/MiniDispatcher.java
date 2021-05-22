package org.apache.flink.runtime.dispatcher;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.clusterframework.ApplicationStatus;
import org.apache.flink.runtime.entrypoint.ClusterEntrypoint;
import org.apache.flink.runtime.entrypoint.JobClusterEntrypoint;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionGraph;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobmaster.JobResult;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.util.FlinkException;

import java.util.Collections;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Mini Dispatcher which is instantiated as the dispatcher component by the {@link JobClusterEntrypoint}.
 *
 * <p>The mini dispatcher is initialized with a single {@link JobGraph} which it runs.
 *
 * <p>Depending on the {@link ClusterEntrypoint.ExecutionMode}, the mini dispatcher will directly
 * terminate after job completion if its execution mode is {@link ClusterEntrypoint.ExecutionMode#DETACHED}.
 */
public class MiniDispatcher extends Dispatcher {

	private final JobClusterEntrypoint.ExecutionMode executionMode;
	private boolean jobCancelled = false;

	public MiniDispatcher(
			RpcService rpcService,
			DispatcherId fencingToken,
			DispatcherServices dispatcherServices,
			JobGraph jobGraph,
			DispatcherBootstrapFactory dispatcherBootstrapFactory,
			JobClusterEntrypoint.ExecutionMode executionMode) throws Exception {
		super(
			rpcService,
			fencingToken,
			Collections.singleton(jobGraph),
			dispatcherBootstrapFactory,
			dispatcherServices);

		this.executionMode = checkNotNull(executionMode);
	}

	@Override
	public CompletableFuture<Acknowledge> submitJob(JobGraph jobGraph, Time timeout) {
		final CompletableFuture<Acknowledge> acknowledgeCompletableFuture = super.submitJob(jobGraph, timeout);

		acknowledgeCompletableFuture.whenComplete(
			(Acknowledge ignored, Throwable throwable) -> {
				if (throwable != null) {
				}
			});

		return acknowledgeCompletableFuture;
	}

	@Override
	public CompletableFuture<JobResult> requestJobResult(JobID jobId, Time timeout) {
		final CompletableFuture<JobResult> jobResultFuture = super.requestJobResult(jobId, timeout);

		if (executionMode == ClusterEntrypoint.ExecutionMode.NORMAL) {
			// terminate the MiniDispatcher once we served the first JobResult successfully
			jobResultFuture.thenAccept((JobResult result) -> {
				ApplicationStatus status = result.getSerializedThrowable().isPresent() ?
						ApplicationStatus.FAILED : ApplicationStatus.SUCCEEDED;

				shutDownFuture.complete(status);
			});
		} 

		return jobResultFuture;
	}

	@Override
	public CompletableFuture<Acknowledge> cancelJob(JobID jobId, Time timeout) {
		jobCancelled = true;
		return super.cancelJob(jobId, timeout);
	}

	@Override
	protected CleanupJobState jobReachedGloballyTerminalState(ArchivedExecutionGraph archivedExecutionGraph) {
		final CleanupJobState cleanupHAState = super.jobReachedGloballyTerminalState(archivedExecutionGraph);

		if (jobCancelled || executionMode == ClusterEntrypoint.ExecutionMode.DETACHED) {
			// shut down if job is cancelled or we don't have to wait for the execution result retrieval
			shutDownFuture.complete(ApplicationStatus.fromJobStatus(archivedExecutionGraph.getState()));
		}

		return cleanupHAState;
	}

	@Override
	protected CleanupJobState jobNotFinished(JobID jobId) {
		final CleanupJobState cleanupJobState = super.jobNotFinished(jobId);

		// shut down since we have done our job
		shutDownFuture.complete(ApplicationStatus.UNKNOWN);

		return cleanupJobState;
	}
}
