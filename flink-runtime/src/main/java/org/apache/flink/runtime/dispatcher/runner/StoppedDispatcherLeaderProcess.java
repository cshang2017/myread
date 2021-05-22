
package org.apache.flink.runtime.dispatcher.runner;

import org.apache.flink.runtime.clusterframework.ApplicationStatus;
import org.apache.flink.runtime.dispatcher.DispatcherGateway;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;

/**
 * {@link DispatcherLeaderProcess} implementation which is stopped. This class
 * is useful as the initial state of the {@link DefaultDispatcherRunner}.
 */
public enum StoppedDispatcherLeaderProcess implements DispatcherLeaderProcess {
	INSTANCE;

	private static final CompletableFuture<Void> TERMINATION_FUTURE = CompletableFuture.completedFuture(null);

	@Override
	public void start() {
		throw new UnsupportedOperationException("This method should never be called.");
	}

	@Override
	public UUID getLeaderSessionId() {
		throw new UnsupportedOperationException("This method should never be called.");
	}

	@Override
	public CompletableFuture<DispatcherGateway> getDispatcherGateway() {
		throw new UnsupportedOperationException("This method should never be called.");
	}

	@Override
	public CompletableFuture<String> getLeaderAddressFuture() {
		throw new UnsupportedOperationException("This method should never be called.");
	}

	@Override
	public CompletableFuture<ApplicationStatus> getShutDownFuture() {
		throw new UnsupportedOperationException("This method should never be called.");
	}

	@Override
	public CompletableFuture<Void> closeAsync() {
		return TERMINATION_FUTURE;
	}
}
