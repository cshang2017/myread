
package org.apache.flink.runtime.dispatcher;

import org.apache.flink.annotation.Internal;

/**
 * An interface containing the logic of bootstrapping the {@link Dispatcher} of a cluster.
 */
@Internal
public interface DispatcherBootstrap {

	/**
	 * Stops and frees any resources (e.g. threads) acquired
	 * during the execution of the bootstrap.
	 */
	void stop() throws Exception;
}
