package org.apache.flink.runtime.dispatcher.runner;

import org.apache.flink.runtime.dispatcher.PartialDispatcherServices;
import org.apache.flink.runtime.jobmanager.JobGraphStoreFactory;
import org.apache.flink.runtime.leaderelection.LeaderElectionService;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.RpcService;

import java.util.concurrent.Executor;

/**
 * Factory interface for the {@link DispatcherRunner}.
 */
public interface DispatcherRunnerFactory {

	DispatcherRunner createDispatcherRunner(
		LeaderElectionService leaderElectionService,
		FatalErrorHandler fatalErrorHandler,
		JobGraphStoreFactory jobGraphStoreFactory,
		Executor ioExecutor,
		RpcService rpcService,
		PartialDispatcherServices partialDispatcherServices) throws Exception;
}
