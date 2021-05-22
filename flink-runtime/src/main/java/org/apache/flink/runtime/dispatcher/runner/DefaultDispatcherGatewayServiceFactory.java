

package org.apache.flink.runtime.dispatcher.runner;

import org.apache.flink.runtime.dispatcher.Dispatcher;
import org.apache.flink.runtime.dispatcher.DispatcherFactory;
import org.apache.flink.runtime.dispatcher.DispatcherId;
import org.apache.flink.runtime.dispatcher.NoOpDispatcherBootstrap;
import org.apache.flink.runtime.dispatcher.PartialDispatcherServices;
import org.apache.flink.runtime.dispatcher.PartialDispatcherServicesWithJobGraphStore;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobmanager.JobGraphWriter;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.util.FlinkRuntimeException;

import java.util.Collection;

/**
 * Factory for the {@link DefaultDispatcherGatewayService}.
 */
class DefaultDispatcherGatewayServiceFactory implements AbstractDispatcherLeaderProcess.DispatcherGatewayServiceFactory {

	private final DispatcherFactory dispatcherFactory;

	private final RpcService rpcService;

	private final PartialDispatcherServices partialDispatcherServices;

	DefaultDispatcherGatewayServiceFactory(
			DispatcherFactory dispatcherFactory,
			RpcService rpcService,
			PartialDispatcherServices partialDispatcherServices) {
		this.dispatcherFactory = dispatcherFactory;
		this.rpcService = rpcService;
		this.partialDispatcherServices = partialDispatcherServices;
	}

	@Override
	public AbstractDispatcherLeaderProcess.DispatcherGatewayService create(
			DispatcherId fencingToken,
			Collection<JobGraph> recoveredJobs,
			JobGraphWriter jobGraphWriter) {

		final Dispatcher dispatcher;
		try {
			dispatcher = dispatcherFactory.createDispatcher(
				rpcService,
				fencingToken,
				recoveredJobs,
				(dispatcherGateway, scheduledExecutor, errorHandler) -> new NoOpDispatcherBootstrap(),
				PartialDispatcherServicesWithJobGraphStore.from(partialDispatcherServices, jobGraphWriter));
		} catch (Exception e) {
			throw new FlinkRuntimeException("Could not create the Dispatcher rpc endpoint.", e);
		}

		dispatcher.start();

		return DefaultDispatcherGatewayService.from(dispatcher);
	}
}
