package org.apache.flink.runtime.dispatcher;

import org.apache.flink.runtime.executiongraph.AccessExecutionGraph;
import org.apache.flink.runtime.messages.Acknowledge;

import java.util.concurrent.CompletableFuture;

/**
 * No-op implementation of the {@link HistoryServerArchivist}.
 */
public enum VoidHistoryServerArchivist implements HistoryServerArchivist {
	INSTANCE;

	@Override
	public CompletableFuture<Acknowledge> archiveExecutionGraph(AccessExecutionGraph executionGraph) {
		return CompletableFuture.completedFuture(Acknowledge.get());
	}
}
