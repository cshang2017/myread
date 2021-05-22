package org.apache.flink.runtime.dispatcher;

import org.apache.flink.annotation.Internal;
import org.apache.flink.runtime.concurrent.ScheduledExecutor;
import org.apache.flink.runtime.rpc.FatalErrorHandler;

/**
 * A factory to create a {@link DispatcherBootstrap}.
 */
@Internal
public interface DispatcherBootstrapFactory {

	DispatcherBootstrap create(
			final DispatcherGateway dispatcher,
			final ScheduledExecutor scheduledExecutor,
			final FatalErrorHandler errorHandler) throws Exception;
}
