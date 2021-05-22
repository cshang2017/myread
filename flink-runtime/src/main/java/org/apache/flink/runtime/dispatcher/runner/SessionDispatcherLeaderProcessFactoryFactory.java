package org.apache.flink.runtime.dispatcher.runner;

import org.apache.flink.runtime.dispatcher.DispatcherFactory;
import org.apache.flink.runtime.dispatcher.PartialDispatcherServices;
import org.apache.flink.runtime.jobmanager.JobGraphStoreFactory;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.RpcService;

import java.util.concurrent.Executor;

/**
 * Factory for the {@link SessionDispatcherLeaderProcessFactory}.
 */
public class SessionDispatcherLeaderProcessFactoryFactory implements DispatcherLeaderProcessFactoryFactory {

	private final DispatcherFactory dispatcherFactory;

	private SessionDispatcherLeaderProcessFactoryFactory(DispatcherFactory dispatcherFactory) {
		this.dispatcherFactory = dispatcherFactory;
	}

	@Override
	public DispatcherLeaderProcessFactory createFactory(
			JobGraphStoreFactory jobGraphStoreFactory,
			Executor ioExecutor,
			RpcService rpcService,
			PartialDispatcherServices partialDispatcherServices,
			FatalErrorHandler fatalErrorHandler) {
		final AbstractDispatcherLeaderProcess.DispatcherGatewayServiceFactory dispatcherGatewayServiceFactory = new DefaultDispatcherGatewayServiceFactory(
			dispatcherFactory,
			rpcService,
			partialDispatcherServices);

		return new SessionDispatcherLeaderProcessFactory(
			dispatcherGatewayServiceFactory,
			jobGraphStoreFactory,
			ioExecutor,
			fatalErrorHandler);
	}

	public static SessionDispatcherLeaderProcessFactoryFactory create(DispatcherFactory dispatcherFactory) {
		return new SessionDispatcherLeaderProcessFactoryFactory(dispatcherFactory);
	}
}
