package org.apache.flink.runtime.dispatcher;

import org.apache.flink.annotation.Internal;
import org.apache.flink.runtime.jobgraph.JobGraph;

/**
 * A {@link DispatcherBootstrap} which submits the provided {@link JobGraph job graphs}
 * for execution upon dispatcher initialization.
 */
@Internal
public class NoOpDispatcherBootstrap implements DispatcherBootstrap {

	public NoOpDispatcherBootstrap() {
	}

	@Override
	public void stop() throws Exception {
		// do nothing
	}
}
