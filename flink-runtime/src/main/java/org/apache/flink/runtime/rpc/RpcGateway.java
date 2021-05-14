

package org.apache.flink.runtime.rpc;

/**
 * Rpc gateway interface which has to be implemented by Rpc gateways.
 */
public interface RpcGateway {

	/**
	 * Returns the fully qualified address under which the associated rpc endpoint is reachable.
	 *
	 * @return Fully qualified (RPC) address under which the associated rpc endpoint is reachable
	 */
	String getAddress();

	/**
	 * Returns the fully qualified hostname under which the associated rpc endpoint is reachable.
	 *
	 * @return Fully qualified hostname under which the associated rpc endpoint is reachable
	 */
	String getHostname();
}
