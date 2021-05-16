package org.apache.flink.runtime.query;

import org.apache.flink.api.common.JobID;
import org.apache.flink.queryablestate.KvStateID;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.state.KeyGroupRange;

/**
 * A listener for a {@link KvStateRegistry}.
 *
 * <p>The registry calls these methods when KvState instances are registered
 * and unregistered.
 */
public interface KvStateRegistryListener {

	/**
	 * Notifies the listener about a registered KvState instance.
	 *
	 * @param jobId            Job ID the KvState instance belongs to
	 * @param jobVertexId      JobVertexID the KvState instance belongs to
	 * @param keyGroupRange    Key group range the KvState instance belongs to
	 * @param registrationName Name under which the KvState is registered
	 * @param kvStateId        ID of the KvState instance
	 */
	void notifyKvStateRegistered(
			JobID jobId,
			JobVertexID jobVertexId,
			KeyGroupRange keyGroupRange,
			String registrationName,
			KvStateID kvStateId);

	/**
	 * Notifies the listener about an unregistered KvState instance.
	 *
	 * @param jobId            Job ID the KvState instance belongs to
	 * @param jobVertexId      JobVertexID the KvState instance belongs to
	 * @param keyGroupRange    Key group range the KvState instance belongs to
	 * @param registrationName Name under which the KvState is registered
	 */
	void notifyKvStateUnregistered(
			JobID jobId,
			JobVertexID jobVertexId,
			KeyGroupRange keyGroupRange,
			String registrationName);

}
