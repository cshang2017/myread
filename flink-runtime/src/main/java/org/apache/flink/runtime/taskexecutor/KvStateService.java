package org.apache.flink.runtime.taskexecutor;

import org.apache.flink.api.common.JobID;
import org.apache.flink.queryablestate.network.stats.DisabledKvStateRequestStats;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.query.KvStateClientProxy;
import org.apache.flink.runtime.query.KvStateRegistry;
import org.apache.flink.runtime.query.KvStateServer;
import org.apache.flink.runtime.query.QueryableStateUtils;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.state.internal.InternalKvState;
import org.apache.flink.util.Preconditions;


/**
 * KvState related components of each {@link TaskExecutor} instance. This service can
 * create the kvState registration for a single task.
 */
public class KvStateService {

	private final Object lock = new Object();

	/** Registry for {@link InternalKvState} instances. */
	private final KvStateRegistry kvStateRegistry;

	/** Server for {@link InternalKvState} requests. */
	private KvStateServer kvStateServer;

	/** Proxy for the queryable state client. */
	private KvStateClientProxy kvStateClientProxy;

	private boolean isShutdown;

	public KvStateService(KvStateRegistry kvStateRegistry, KvStateServer kvStateServer, KvStateClientProxy kvStateClientProxy) {
		this.kvStateRegistry = Preconditions.checkNotNull(kvStateRegistry);
		this.kvStateServer = kvStateServer;
		this.kvStateClientProxy = kvStateClientProxy;
	}

	// --------------------------------------------------------------------------------------------
	//  Getter/Setter
	// --------------------------------------------------------------------------------------------

	public KvStateRegistry getKvStateRegistry() {
		return kvStateRegistry;
	}

	public KvStateServer getKvStateServer() {
		return kvStateServer;
	}

	public KvStateClientProxy getKvStateClientProxy() {
		return kvStateClientProxy;
	}

	public TaskKvStateRegistry createKvStateTaskRegistry(JobID jobId, JobVertexID jobVertexId) {
		return kvStateRegistry.createTaskRegistry(jobId, jobVertexId);
	}

	// --------------------------------------------------------------------------------------------
	//  Start and shut down methods
	// --------------------------------------------------------------------------------------------

	public void start() {
		synchronized (lock) {
			Preconditions.checkState(!isShutdown, "The KvStateService has already been shut down.");

					kvStateServer.start();

					kvStateClientProxy.start();
		}
	}

	public void shutdown() {
		synchronized (lock) {
			if (isShutdown) {
				return;
			}

					kvStateClientProxy.shutdown();

					kvStateServer.shutdown();

			isShutdown = true;
		}
	}

	public boolean isShutdown() {
		synchronized (lock) {
			return isShutdown;
		}
	}

	// --------------------------------------------------------------------------------------------
	//  Static factory methods for kvState service
	// --------------------------------------------------------------------------------------------

	/**
	 * Creates and returns the KvState service.
	 *
	 * @param taskManagerServicesConfiguration task manager configuration
	 * @return service for kvState related components
	 */
	public static KvStateService fromConfiguration(TaskManagerServicesConfiguration taskManagerServicesConfiguration) {
		KvStateRegistry kvStateRegistry = new KvStateRegistry();

		QueryableStateConfiguration qsConfig = taskManagerServicesConfiguration.getQueryableStateConfig();

		KvStateClientProxy kvClientProxy = null;
		KvStateServer kvStateServer = null;

		if (qsConfig != null) {
			int numProxyServerNetworkThreads = qsConfig.numProxyServerThreads() == 0 ?
				taskManagerServicesConfiguration.getNumberOfSlots() : qsConfig.numProxyServerThreads();
			int numProxyServerQueryThreads = qsConfig.numProxyQueryThreads() == 0 ?
				taskManagerServicesConfiguration.getNumberOfSlots() : qsConfig.numProxyQueryThreads();
			kvClientProxy = QueryableStateUtils.createKvStateClientProxy(
				taskManagerServicesConfiguration.getExternalAddress(),
				qsConfig.getProxyPortRange(),
				numProxyServerNetworkThreads,
				numProxyServerQueryThreads,
				new DisabledKvStateRequestStats());

			int numStateServerNetworkThreads = qsConfig.numStateServerThreads() == 0 ?
				taskManagerServicesConfiguration.getNumberOfSlots() : qsConfig.numStateServerThreads();
			int numStateServerQueryThreads = qsConfig.numStateQueryThreads() == 0 ?
				taskManagerServicesConfiguration.getNumberOfSlots() : qsConfig.numStateQueryThreads();
			kvStateServer = QueryableStateUtils.createKvStateServer(
				taskManagerServicesConfiguration.getExternalAddress(),
				qsConfig.getStateServerPortRange(),
				numStateServerNetworkThreads,
				numStateServerQueryThreads,
				kvStateRegistry,
				new DisabledKvStateRequestStats());
		}

		return new KvStateService(kvStateRegistry, kvStateServer, kvClientProxy);
	}
}
