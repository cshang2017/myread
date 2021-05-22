package org.apache.flink.runtime.dispatcher;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.executiongraph.AccessExecutionGraph;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.webmonitor.WebMonitorUtils;
import org.apache.flink.runtime.webmonitor.history.JsonArchivist;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/**
 * Writer for an {@link AccessExecutionGraph}.
 */
public interface HistoryServerArchivist {

	/**
	 * Archives the given {@link AccessExecutionGraph} on the history server.
	 *
	 * @param executionGraph to store on the history server
	 * @return Future which is completed once the archiving has been completed.
	 */
	CompletableFuture<Acknowledge> archiveExecutionGraph(AccessExecutionGraph executionGraph);

	static HistoryServerArchivist createHistoryServerArchivist(Configuration configuration, JsonArchivist jsonArchivist, Executor ioExecutor) {
		final String configuredArchivePath = configuration.getString(JobManagerOptions.ARCHIVE_DIR);

		if (configuredArchivePath != null) {
			final Path archivePath = WebMonitorUtils.validateAndNormalizeUri(new Path(configuredArchivePath).toUri());

			return new JsonResponseHistoryServerArchivist(jsonArchivist, archivePath, ioExecutor);
		} else {
			return VoidHistoryServerArchivist.INSTANCE;
		}
	}
}
