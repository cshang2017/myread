

package org.apache.flink.runtime.dispatcher.runner;

import org.apache.flink.runtime.clusterframework.ApplicationStatus;
import org.apache.flink.runtime.dispatcher.Dispatcher;
import org.apache.flink.util.AutoCloseableAsync;

import java.util.concurrent.CompletableFuture;

/**
 * The {@link DispatcherRunner} encapsulates how a {@link Dispatcher} is being executed.
 */
public interface DispatcherRunner extends AutoCloseableAsync {

	/**
	 * Return shut down future of this runner. The shut down future is being
	 * completed with the final {@link ApplicationStatus} once the runner wants
	 * to shut down.
	 *
	 * @return future with the final application status
	 */
	CompletableFuture<ApplicationStatus> getShutDownFuture();
}
