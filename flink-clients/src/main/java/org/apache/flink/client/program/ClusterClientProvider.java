
package org.apache.flink.client.program;

import org.apache.flink.annotation.Internal;

/**
 * Factory for {@link ClusterClient ClusterClients}.
 */
@Internal
public interface ClusterClientProvider<T> {

	/**
	 * Creates and returns a new {@link ClusterClient}. The returned client needs to be closed via
	 * {@link ClusterClient#close()} after use.
	 */
	ClusterClient<T> getClusterClient();
}
