package org.apache.flink.runtime.jobmanager;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.state.RetrievableStateHandle;
import org.apache.flink.runtime.zookeeper.ZooKeeperStateHandleStore;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkException;

import org.apache.flink.shaded.curator4.org.apache.curator.framework.CuratorFramework;
import org.apache.flink.shaded.curator4.org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.flink.shaded.curator4.org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.flink.shaded.curator4.org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.flink.shaded.curator4.org.apache.curator.utils.ZKPaths;
import org.apache.flink.shaded.zookeeper3.org.apache.zookeeper.KeeperException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * {@link JobGraph} instances for JobManagers running in {@link HighAvailabilityMode#ZOOKEEPER}.
 *
 * <p>Each job graph creates ZNode:
 * <pre>
 * +----O /flink/jobgraphs/&lt;job-id&gt; 1 [persistent]
 * .
 * .
 * .
 * +----O /flink/jobgraphs/&lt;job-id&gt; N [persistent]
 * </pre>
 *
 * <p>The root path is watched to detect concurrent modifications in corner situations where
 * multiple instances operate concurrently. The job manager acts as a {@link JobGraphListener}
 * to react to such situations.
 */
public class ZooKeeperJobGraphStore implements JobGraphStore {

	/** Lock to synchronize with the {@link JobGraphListener}. */
	private final Object cacheLock = new Object();

	/** The set of IDs of all added job graphs. */
	private final Set<JobID> addedJobGraphs = new HashSet<>();

	/** Submitted job graphs in ZooKeeper. */
	private final ZooKeeperStateHandleStore<JobGraph> jobGraphsInZooKeeper;

	/**
	 * Cache to monitor all children. This is used to detect races with other instances working
	 * on the same state.
	 */
	private final PathChildrenCache pathCache;

	/** The full configured base path including the namespace. */
	private final String zooKeeperFullBasePath;

	/** The external listener to be notified on races. */
	private JobGraphListener jobGraphListener;

	/** Flag indicating whether this instance is running. */
	private boolean isRunning;

	/**
	 * Submitted job graph store backed by ZooKeeper.
	 *
	 * @param zooKeeperFullBasePath ZooKeeper path for current job graphs
	 * @param zooKeeperStateHandleStore State storage used to persist the submitted jobs
	 */
	public ZooKeeperJobGraphStore(
			String zooKeeperFullBasePath,
			ZooKeeperStateHandleStore<JobGraph> zooKeeperStateHandleStore,
			PathChildrenCache pathCache) {

		this.zooKeeperFullBasePath = zooKeeperFullBasePath;
		this.jobGraphsInZooKeeper = checkNotNull(zooKeeperStateHandleStore);

		this.pathCache = checkNotNull(pathCache);
		pathCache.getListenable().addListener(new JobGraphsPathCacheListener());
	}

	@Override
	public void start(JobGraphListener jobGraphListener) throws Exception {
		synchronized (cacheLock) {
			if (!isRunning) {
				this.jobGraphListener = jobGraphListener;

				pathCache.start();

				isRunning = true;
			}
		}
	}

	@Override
	public void stop() throws Exception {
		synchronized (cacheLock) {
			if (isRunning) {
				jobGraphListener = null;

					Exception exception = null;

						jobGraphsInZooKeeper.releaseAll();

						pathCache.close();

					isRunning = false;
			}
		}
	}

	@Override
	@Nullable
	public JobGraph recoverJobGraph(JobID jobId) throws Exception {
		checkNotNull(jobId, "Job ID");
		final String path = getPathForJob(jobId);

		synchronized (cacheLock) {
			verifyIsRunning();

			boolean success = false;

			try {
				RetrievableStateHandle<JobGraph> 
					jobGraphRetrievableStateHandle = jobGraphsInZooKeeper.getAndLock(path);
				JobGraph jobGraph = jobGraphRetrievableStateHandle.retrieveState();

				addedJobGraphs.add(jobGraph.getJobID());

				success = true;
				return jobGraph;
			} finally {
				if (!success) {
					jobGraphsInZooKeeper.release(path);
				}
			}
		}
	}

	@Override
	public void putJobGraph(JobGraph jobGraph) throws Exception {
		checkNotNull(jobGraph, "Job graph");
		String path = getPathForJob(jobGraph.getJobID());

		boolean success = false;

		while (!success) {
			synchronized (cacheLock) {
				verifyIsRunning();

				int currentVersion = jobGraphsInZooKeeper.exists(path);

				if (currentVersion == -1) {
						jobGraphsInZooKeeper.addAndLock(path, jobGraph);

						addedJobGraphs.add(jobGraph.getJobID());

						success = true;
				}
				else if (addedJobGraphs.contains(jobGraph.getJobID())) {
						jobGraphsInZooKeeper.replace(path, currentVersion, jobGraph);

						success = true;
				}
			}
		}

	}

	@Override
	public void removeJobGraph(JobID jobId) throws Exception {
		checkNotNull(jobId, "Job ID");
		String path = getPathForJob(jobId);


		synchronized (cacheLock) {
			if (addedJobGraphs.contains(jobId)) {
				if (jobGraphsInZooKeeper.releaseAndTryRemove(path)) {
					addedJobGraphs.remove(jobId);
				}
			}
		}

	}

	@Override
	public void releaseJobGraph(JobID jobId) throws Exception {
		final String path = getPathForJob(jobId);

		synchronized (cacheLock) {
			if (addedJobGraphs.contains(jobId)) {
				jobGraphsInZooKeeper.release(path);

				addedJobGraphs.remove(jobId);
			}
		}
	}

	@Override
	public Collection<JobID> getJobIds() throws Exception {
		Collection<String> paths;

			paths = jobGraphsInZooKeeper.getAllPaths();

		List<JobID> jobIds = new ArrayList<>(paths.size());

		for (String path : paths) {
				jobIds.add(jobIdfromPath(path));
		}

		return jobIds;
	}

	/**
	 * Monitors ZooKeeper for changes.
	 *
	 * <p>Detects modifications from other job managers in corner situations. The event
	 * notifications fire for changes from this job manager as well.
	 */
	private final class JobGraphsPathCacheListener implements PathChildrenCacheListener {

		@Override
		public void childEvent(CuratorFramework client, PathChildrenCacheEvent event)
				throws Exception {
			switch (event.getType()) {
				case CHILD_ADDED: {
					JobID jobId = fromEvent(event);

					synchronized (cacheLock) {
							if (jobGraphListener != null && !addedJobGraphs.contains(jobId)) {
									jobGraphListener.onAddedJobGraph(jobId);
							}
					}
				}
				break;

				case CHILD_UPDATED: {
					// Nothing to do
				}
				break;

				case CHILD_REMOVED: {
					JobID jobId = fromEvent(event);

					synchronized (cacheLock) {
							if (jobGraphListener != null && addedJobGraphs.contains(jobId)) {
									jobGraphListener.onRemovedJobGraph(jobId);
							}

							break;
					}
				}
				break;

				case CONNECTION_SUSPENDED: {
					LOG.warn("ZooKeeper connection SUSPENDING. Changes to the submitted job " +
						"graphs are not monitored (temporarily).");
				}
				break;

				case CONNECTION_LOST: {
					LOG.warn("ZooKeeper connection LOST. Changes to the submitted job " +
						"graphs are not monitored (permanently).");
				}
				break;

				case CONNECTION_RECONNECTED: {
					LOG.info("ZooKeeper connection RECONNECTED. Changes to the submitted job " +
						"graphs are monitored again.");
				}
				break;

				case INITIALIZED: {
					LOG.info("JobGraphsPathCacheListener initialized");
				}
				break;
			}
		}

		/**
		 * Returns a JobID for the event's path.
		 */
		private JobID fromEvent(PathChildrenCacheEvent event) {
			return JobID.fromHexString(ZKPaths.getNodeFromPath(event.getData().getPath()));
		}
	}

	/**
	 * Verifies that the state is running.
	 */
	private void verifyIsRunning() {
		checkState(isRunning, "Not running. Forgot to call start()?");
	}

	/**
	 * Returns the JobID as a String (with leading slash).
	 */
	public static String getPathForJob(JobID jobId) {
		checkNotNull(jobId, "Job ID");
		return String.format("/%s", jobId);
	}

	/**
	 * Returns the JobID from the given path in ZooKeeper.
	 *
	 * @param path in ZooKeeper
	 * @return JobID associated with the given path
	 */
	public static JobID jobIdfromPath(final String path) {
		return JobID.fromHexString(path);
	}
}
