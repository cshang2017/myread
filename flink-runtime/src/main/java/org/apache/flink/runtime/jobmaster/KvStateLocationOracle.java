package org.apache.flink.runtime.jobmaster;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.query.KvStateLocation;
import org.apache.flink.runtime.state.internal.InternalKvState;
import org.apache.flink.runtime.taskexecutor.TaskExecutor;

import java.util.concurrent.CompletableFuture;

/**
 * Oracle for {@link KvStateLocation} in the cluster. In order to answer {@link InternalKvState}
 * location requests, the {@link TaskExecutor} have to register and unregister their respective
 * key-value states at the oracle.
 */
public interface KvStateLocationOracle {

	/**
	 * Requests a {@link KvStateLocation} for the specified {@link InternalKvState} registration name.
	 *
	 * @param jobId identifying the job for which to request the {@link KvStateLocation}
	 * @param registrationName Name under which the KvState has been registered.
	 * @return Future of the requested {@link InternalKvState} location
	 */
	CompletableFuture<KvStateLocation> requestKvStateLocation(
		final JobID jobId,
		final String registrationName);
}
