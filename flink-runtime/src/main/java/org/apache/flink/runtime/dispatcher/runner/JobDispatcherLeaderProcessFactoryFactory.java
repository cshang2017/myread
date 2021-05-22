
package org.apache.flink.runtime.dispatcher.runner;

import org.apache.flink.runtime.dispatcher.JobDispatcherFactory;
import org.apache.flink.runtime.dispatcher.PartialDispatcherServices;
import org.apache.flink.runtime.entrypoint.component.JobGraphRetriever;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobmanager.JobGraphStoreFactory;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.FlinkRuntimeException;

import java.util.concurrent.Executor;

/**
 * Factory for the {@link JobDispatcherLeaderProcessFactory}.
 */
public class JobDispatcherLeaderProcessFactoryFactory implements DispatcherLeaderProcessFactoryFactory {

	private final JobGraphRetriever jobGraphRetriever;

	private JobDispatcherLeaderProcessFactoryFactory(JobGraphRetriever jobGraphRetriever) {
		this.jobGraphRetriever = jobGraphRetriever;
	}

	@Override
	public DispatcherLeaderProcessFactory createFactory(
			JobGraphStoreFactory jobGraphStoreFactory,
			Executor ioExecutor,
			RpcService rpcService,
			PartialDispatcherServices partialDispatcherServices,
			FatalErrorHandler fatalErrorHandler) {

		final JobGraph jobGraph;

			jobGraph = jobGraphRetriever.retrieveJobGraph(partialDispatcherServices.getConfiguration());

		final DefaultDispatcherGatewayServiceFactory defaultDispatcherServiceFactory = new DefaultDispatcherGatewayServiceFactory(
			JobDispatcherFactory.INSTANCE,
			rpcService,
			partialDispatcherServices);

		return new JobDispatcherLeaderProcessFactory(
			defaultDispatcherServiceFactory,
			jobGraph,
			fatalErrorHandler);
	}

	public static JobDispatcherLeaderProcessFactoryFactory create(JobGraphRetriever jobGraphRetriever) {
		return new JobDispatcherLeaderProcessFactoryFactory(jobGraphRetriever);
	}
}
