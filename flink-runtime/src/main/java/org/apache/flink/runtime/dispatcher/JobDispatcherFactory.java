package org.apache.flink.runtime.dispatcher;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.entrypoint.ClusterEntrypoint;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.rpc.RpcService;

import org.apache.flink.shaded.guava18.com.google.common.collect.Iterables;

import java.util.Collection;

import static org.apache.flink.runtime.entrypoint.ClusterEntrypoint.EXECUTION_MODE;

/**
 * {@link DispatcherFactory} which creates a {@link MiniDispatcher}.
 */
public enum JobDispatcherFactory implements DispatcherFactory {
	INSTANCE;

	@Override
	public MiniDispatcher createDispatcher(
			RpcService rpcService,
			DispatcherId fencingToken,
			Collection<JobGraph> recoveredJobs,
			DispatcherBootstrapFactory dispatcherBootstrapFactory,
			PartialDispatcherServicesWithJobGraphStore partialDispatcherServicesWithJobGraphStore) throws Exception {
		final JobGraph jobGraph = Iterables.getOnlyElement(recoveredJobs);

		final Configuration configuration = partialDispatcherServicesWithJobGraphStore.getConfiguration();
		final String executionModeValue = configuration.getString(EXECUTION_MODE);
		final ClusterEntrypoint.ExecutionMode executionMode = ClusterEntrypoint.ExecutionMode.valueOf(executionModeValue);

		return new MiniDispatcher(
			rpcService,
			fencingToken,
			DispatcherServices.from(partialDispatcherServicesWithJobGraphStore, DefaultJobManagerRunnerFactory.INSTANCE),
			jobGraph,
			dispatcherBootstrapFactory,
			executionMode);
	}
}
