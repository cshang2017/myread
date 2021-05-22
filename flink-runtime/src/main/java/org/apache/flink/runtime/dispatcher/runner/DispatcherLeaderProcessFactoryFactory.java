
package org.apache.flink.runtime.dispatcher.runner;

import org.apache.flink.annotation.Internal;
import org.apache.flink.runtime.dispatcher.PartialDispatcherServices;
import org.apache.flink.runtime.jobmanager.JobGraphStoreFactory;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.RpcService;

import java.util.concurrent.Executor;

/**
 * Factory for {@link DispatcherLeaderProcessFactory}.
 */
@Internal
public interface DispatcherLeaderProcessFactoryFactory {

	DispatcherLeaderProcessFactory createFactory(
		JobGraphStoreFactory jobGraphStoreFactory,
		Executor ioExecutor,
		RpcService rpcService,
		PartialDispatcherServices partialDispatcherServices,
		FatalErrorHandler fatalErrorHandler);
}
