package org.apache.flink.runtime.jobmaster;

import org.apache.flink.api.common.JobID;
import org.apache.flink.queryablestate.KvStateID;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.state.KeyGroupRange;

import java.net.InetSocketAddress;
import java.util.concurrent.CompletableFuture;

/**
 * Gateway to report key-value state registration and deregistrations.
 */
public interface KvStateRegistryGateway {

	/**
	 * Notifies that queryable state has been registered.
	 *
	 * @param jobId	identifying the job for which to register a key value state
	 * @param jobVertexId JobVertexID the KvState instance belongs to.
	 * @param keyGroupRange Key group range the KvState instance belongs to.
	 * @param registrationName Name under which the KvState has been registered.
	 * @param kvStateId ID of the registered KvState instance.
	 * @param kvStateServerAddress Server address where to find the KvState instance.
	 * @return Future acknowledge if the key-value state has been registered
	 */
	CompletableFuture<Acknowledge> notifyKvStateRegistered(
		final JobID jobId,
		final JobVertexID jobVertexId,
		final KeyGroupRange keyGroupRange,
		final String registrationName,
		final KvStateID kvStateId,
		final InetSocketAddress kvStateServerAddress);

	/**
	 * Notifies that queryable state has been unregistered.
	 *
	 * @param jobId	identifying the job for which to unregister a key value state
	 * @param jobVertexId JobVertexID the KvState instance belongs to.
	 * @param keyGroupRange Key group index the KvState instance belongs to.
	 * @param registrationName Name under which the KvState has been registered.
	 * @return Future acknowledge if the key-value state has been unregistered
	 */
	CompletableFuture<Acknowledge> notifyKvStateUnregistered(
		final JobID jobId,
		final JobVertexID jobVertexId,
		final KeyGroupRange keyGroupRange,
		final String registrationName);
}
