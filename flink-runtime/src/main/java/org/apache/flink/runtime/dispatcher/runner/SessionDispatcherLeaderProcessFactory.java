

package org.apache.flink.runtime.dispatcher.runner;

import org.apache.flink.annotation.Internal;
import org.apache.flink.runtime.jobmanager.JobGraphStoreFactory;
import org.apache.flink.runtime.rpc.FatalErrorHandler;

import java.util.UUID;
import java.util.concurrent.Executor;

/**
 * Factory for the {@link SessionDispatcherLeaderProcess}.
 */
@Internal
public class SessionDispatcherLeaderProcessFactory implements DispatcherLeaderProcessFactory {

	private final AbstractDispatcherLeaderProcess.DispatcherGatewayServiceFactory dispatcherGatewayServiceFactory;
	private final JobGraphStoreFactory jobGraphStoreFactory;
	private final Executor ioExecutor;
	private final FatalErrorHandler fatalErrorHandler;

	public SessionDispatcherLeaderProcessFactory(
			AbstractDispatcherLeaderProcess.DispatcherGatewayServiceFactory dispatcherGatewayServiceFactory,
			JobGraphStoreFactory jobGraphStoreFactory,
			Executor ioExecutor,
			FatalErrorHandler fatalErrorHandler) {
		this.dispatcherGatewayServiceFactory = dispatcherGatewayServiceFactory;
		this.jobGraphStoreFactory = jobGraphStoreFactory;
		this.ioExecutor = ioExecutor;
		this.fatalErrorHandler = fatalErrorHandler;
	}

	@Override
	public DispatcherLeaderProcess create(UUID leaderSessionID) {
		return SessionDispatcherLeaderProcess.create(
			leaderSessionID,
			dispatcherGatewayServiceFactory,
			jobGraphStoreFactory.create(),
			ioExecutor,
			fatalErrorHandler);
	}
}
