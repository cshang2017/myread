package org.apache.flink.runtime.dispatcher;

import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.rpc.RpcService;

import java.util.Collection;

/**
 * {@link Dispatcher} factory interface.
 */
public interface DispatcherFactory {

	/**
	 * Create a {@link Dispatcher}.
	 */
	Dispatcher createDispatcher(
			RpcService rpcService,
			DispatcherId fencingToken,
			Collection<JobGraph> recoveredJobs,
			DispatcherBootstrapFactory dispatcherBootstrapFactory,
			PartialDispatcherServicesWithJobGraphStore partialDispatcherServicesWithJobGraphStore) throws Exception;
}
