package org.apache.flink.runtime.dispatcher;

import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.rpc.RpcService;

import java.util.Collection;

/**
 * {@link DispatcherFactory} which creates a {@link StandaloneDispatcher}.
 */
public enum SessionDispatcherFactory implements DispatcherFactory {
	INSTANCE;

	@Override
	public StandaloneDispatcher createDispatcher(
			RpcService rpcService,
			DispatcherId fencingToken,
			Collection<JobGraph> recoveredJobs,
			DispatcherBootstrapFactory dispatcherBootstrapFactory,
			PartialDispatcherServicesWithJobGraphStore partialDispatcherServicesWithJobGraphStore) throws Exception {
		// create the default dispatcher
		return new StandaloneDispatcher(
			rpcService,
			fencingToken,
			recoveredJobs,
			dispatcherBootstrapFactory,
			DispatcherServices.from(partialDispatcherServicesWithJobGraphStore, DefaultJobManagerRunnerFactory.INSTANCE));
	}
}
