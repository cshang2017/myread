

package org.apache.flink.runtime.dispatcher.runner;

import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.rpc.FatalErrorHandler;

import java.util.UUID;

/**
 * Factory for the {@link JobDispatcherLeaderProcess}.
 */
public class JobDispatcherLeaderProcessFactory implements DispatcherLeaderProcessFactory {
	private final AbstractDispatcherLeaderProcess.DispatcherGatewayServiceFactory dispatcherGatewayServiceFactory;

	private final JobGraph jobGraph;

	private final FatalErrorHandler fatalErrorHandler;

	JobDispatcherLeaderProcessFactory(
			AbstractDispatcherLeaderProcess.DispatcherGatewayServiceFactory dispatcherGatewayServiceFactory,
			JobGraph jobGraph,
			FatalErrorHandler fatalErrorHandler) {
		this.dispatcherGatewayServiceFactory = dispatcherGatewayServiceFactory;
		this.jobGraph = jobGraph;
		this.fatalErrorHandler = fatalErrorHandler;
	}

	@Override
	public DispatcherLeaderProcess create(UUID leaderSessionID) {
		return new JobDispatcherLeaderProcess(leaderSessionID, dispatcherGatewayServiceFactory, jobGraph, fatalErrorHandler);
	}
}
