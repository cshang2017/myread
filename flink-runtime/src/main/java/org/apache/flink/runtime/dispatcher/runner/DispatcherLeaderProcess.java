
package org.apache.flink.runtime.dispatcher.runner;

import org.apache.flink.runtime.clusterframework.ApplicationStatus;
import org.apache.flink.runtime.dispatcher.Dispatcher;
import org.apache.flink.runtime.dispatcher.DispatcherGateway;
import org.apache.flink.util.AutoCloseableAsync;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;

/**
 * Leader process which encapsulates the lifecycle of the {@link Dispatcher} component.
 */
interface DispatcherLeaderProcess extends AutoCloseableAsync {

	void start();

	UUID getLeaderSessionId();

	CompletableFuture<DispatcherGateway> getDispatcherGateway();

	CompletableFuture<String> getLeaderAddressFuture();

	CompletableFuture<ApplicationStatus> getShutDownFuture();
}
