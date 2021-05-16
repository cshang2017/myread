

package org.apache.flink.runtime.query;

import java.net.InetSocketAddress;

/**
 * An interface for the Queryable State Server running on each Task Manager in the cluster.
 * This server is responsible for serving requests coming from the {@link KvStateClientProxy
 * Queryable State Proxy} and requesting <b>locally</b> stored state.
 */
public interface KvStateServer {

	/**
	 * Returns the {@link InetSocketAddress address} the server is listening to.
	 * @return Server address.
	 */
	InetSocketAddress getServerAddress();


	/** Starts the server. */
	void start() throws Throwable;

	/** Shuts down the server and all related thread pools. */
	void shutdown();
}
