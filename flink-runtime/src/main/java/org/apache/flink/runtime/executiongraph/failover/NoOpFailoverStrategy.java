package org.apache.flink.runtime.executiongraph.failover;

import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;

import java.util.List;

/**
 * FailoverStrategy that does not do anything.
 */
public class NoOpFailoverStrategy extends FailoverStrategy {

	@Override
	public void onTaskFailure(final Execution taskExecution, final Throwable cause) {
	}

	@Override
	public void notifyNewVertices(final List<ExecutionJobVertex> newJobVerticesTopological) {
	}

	@Override
	public String getStrategyName() {
		return "NoOp failover strategy";
	}

	public static class Factory implements FailoverStrategy.Factory {

		@Override
		public FailoverStrategy create(final ExecutionGraph executionGraph) {
			return new NoOpFailoverStrategy();
		}
	}
}
