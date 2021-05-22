package org.apache.flink.runtime.dispatcher;

import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.executiongraph.AccessExecutionGraph;
import org.apache.flink.runtime.history.FsJobArchivist;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.webmonitor.history.JsonArchivist;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.function.ThrowingRunnable;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/**
 * Implementation which archives an {@link AccessExecutionGraph} such that it stores
 * the JSON requests for all possible history server requests.
 */
class JsonResponseHistoryServerArchivist implements HistoryServerArchivist {

	private final JsonArchivist jsonArchivist;

	private final Path archivePath;

	private final Executor ioExecutor;

	JsonResponseHistoryServerArchivist(JsonArchivist jsonArchivist, Path archivePath, Executor ioExecutor) {
		this.jsonArchivist = Preconditions.checkNotNull(jsonArchivist);
		this.archivePath = Preconditions.checkNotNull(archivePath);
		this.ioExecutor = Preconditions.checkNotNull(ioExecutor);
	}

	@Override
	public CompletableFuture<Acknowledge> archiveExecutionGraph(AccessExecutionGraph executionGraph) {
		return CompletableFuture
			.runAsync(
				ThrowingRunnable.unchecked(() ->
					FsJobArchivist.archiveJob(archivePath, executionGraph.getJobID(), jsonArchivist.archiveJsonWithPath(executionGraph))),
				ioExecutor)
			.thenApply(ignored -> Acknowledge.get());
	}
}
