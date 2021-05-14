package org.apache.flink.runtime.taskexecutor.rpc;

import org.apache.flink.api.common.JobID;
import org.apache.flink.queryablestate.KvStateID;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobmaster.KvStateRegistryGateway;
import org.apache.flink.runtime.query.KvStateRegistryListener;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.util.Preconditions;

import java.net.InetSocketAddress;

/**
 * {@link KvStateRegistryListener} implementation for the new RPC service.
 */
public class RpcKvStateRegistryListener implements KvStateRegistryListener {

	private final KvStateRegistryGateway kvStateRegistryGateway;
	private final InetSocketAddress kvStateServerAddress;

	public RpcKvStateRegistryListener(
			KvStateRegistryGateway kvStateRegistryGateway,
			InetSocketAddress kvStateServerAddress) {
		this.kvStateRegistryGateway = Preconditions.checkNotNull(kvStateRegistryGateway);
		this.kvStateServerAddress = Preconditions.checkNotNull(kvStateServerAddress);
	}

	@Override
	public void notifyKvStateRegistered(
			JobID jobId,
			JobVertexID jobVertexId,
			KeyGroupRange keyGroupRange,
			String registrationName,
			KvStateID kvStateId) {
		kvStateRegistryGateway.notifyKvStateRegistered(
			jobId,
			jobVertexId,
			keyGroupRange,
			registrationName,
			kvStateId,
			kvStateServerAddress);
	}

	@Override
	public void notifyKvStateUnregistered(
		JobID jobId,
		JobVertexID jobVertexId,
		KeyGroupRange keyGroupRange,
		String registrationName) {

		kvStateRegistryGateway.notifyKvStateUnregistered(
			jobId,
			jobVertexId,
			keyGroupRange,
			registrationName);
	}
}
