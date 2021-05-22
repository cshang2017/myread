package org.apache.flink.runtime.dispatcher.runner;

import org.apache.flink.runtime.clusterframework.ApplicationStatus;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.leaderelection.LeaderContender;
import org.apache.flink.runtime.leaderelection.LeaderElectionService;

import java.util.Arrays;
import java.util.concurrent.CompletableFuture;

final class DispatcherRunnerLeaderElectionLifecycleManager<T extends DispatcherRunner & LeaderContender> implements DispatcherRunner {
	private final T dispatcherRunner;
	private final LeaderElectionService leaderElectionService;

	private DispatcherRunnerLeaderElectionLifecycleManager(T dispatcherRunner, LeaderElectionService leaderElectionService) throws Exception {
		this.dispatcherRunner = dispatcherRunner;
		this.leaderElectionService = leaderElectionService;

		leaderElectionService.start(dispatcherRunner);
	}

	@Override
	public CompletableFuture<ApplicationStatus> getShutDownFuture() {
		return dispatcherRunner.getShutDownFuture();
	}

	@Override
	public CompletableFuture<Void> closeAsync() {
		final CompletableFuture<Void> servicesTerminationFuture = stopServices();
		final CompletableFuture<Void> dispatcherRunnerTerminationFuture = dispatcherRunner.closeAsync();

		return FutureUtils.completeAll(Arrays.asList(servicesTerminationFuture, dispatcherRunnerTerminationFuture));
	}

	private CompletableFuture<Void> stopServices() {
		try {
			leaderElectionService.stop();
		} catch (Exception e) {
			return FutureUtils.completedExceptionally(e);
		}

		return FutureUtils.completedVoidFuture();
	}

	public static <T extends DispatcherRunner & LeaderContender> DispatcherRunner createFor(T dispatcherRunner, LeaderElectionService leaderElectionService) throws Exception {
		return new DispatcherRunnerLeaderElectionLifecycleManager<>(dispatcherRunner, leaderElectionService);
	}
}
