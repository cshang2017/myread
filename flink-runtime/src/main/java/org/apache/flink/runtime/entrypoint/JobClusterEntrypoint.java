package org.apache.flink.runtime.entrypoint;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.concurrent.ScheduledExecutor;
import org.apache.flink.runtime.dispatcher.ArchivedExecutionGraphStore;
import org.apache.flink.runtime.dispatcher.MemoryArchivedExecutionGraphStore;

/**
 * Base class for per-job cluster entry points.
 */
public abstract class JobClusterEntrypoint extends ClusterEntrypoint {

	public JobClusterEntrypoint(Configuration configuration) {
		super(configuration);
	}

	@Override
	protected ArchivedExecutionGraphStore createSerializableExecutionGraphStore(
			Configuration configuration,
			ScheduledExecutor scheduledExecutor) {
		return new MemoryArchivedExecutionGraphStore();
	}
}
