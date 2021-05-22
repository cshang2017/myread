package org.apache.flink.runtime.dispatcher.runner;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.clusterframework.ApplicationStatus;
import org.apache.flink.runtime.dispatcher.Dispatcher;
import org.apache.flink.runtime.dispatcher.DispatcherGateway;

import java.util.concurrent.CompletableFuture;

/**
 * A facade over the {@link Dispatcher} that exposes specific functionality.
 */
@Internal
public class DefaultDispatcherGatewayService implements AbstractDispatcherLeaderProcess.DispatcherGatewayService {

	private final Dispatcher dispatcher;
	private final DispatcherGateway dispatcherGateway;

	private DefaultDispatcherGatewayService(Dispatcher dispatcher) {
		this.dispatcher = dispatcher;
		this.dispatcherGateway = dispatcher.getSelfGateway(DispatcherGateway.class);
	}

	@Override
	public DispatcherGateway getGateway() {
		return dispatcherGateway;
	}

	@Override
	public CompletableFuture<Void> onRemovedJobGraph(JobID jobId) {
		return dispatcher.onRemovedJobGraph(jobId);
	}

	@Override
	public CompletableFuture<ApplicationStatus> getShutDownFuture() {
		return dispatcher.getShutDownFuture();
	}

	@Override
	public CompletableFuture<Void> getTerminationFuture() {
		return dispatcher.getTerminationFuture();
	}

	@Override
	public CompletableFuture<Void> closeAsync() {
		return dispatcher.closeAsync();
	}

	public static DefaultDispatcherGatewayService from(Dispatcher dispatcher) {
		return new DefaultDispatcherGatewayService(dispatcher);
	}
}
