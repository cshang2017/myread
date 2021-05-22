package org.apache.flink.runtime.executiongraph.failover;

import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;

import java.util.List;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Simple failover strategy that triggers a restart of all tasks in the
 * execution graph, via {@link ExecutionGraph#failGlobal(Throwable)}.
 */
public class RestartAllStrategy extends FailoverStrategy {

	/** The execution graph to recover */
	private final ExecutionGraph executionGraph;

	/**
	 * Creates a new failover strategy that recovers from failures by restarting all tasks
	 * of the execution graph.
	 * 
	 * @param executionGraph The execution graph to handle.
	 */
	public RestartAllStrategy(ExecutionGraph executionGraph) {
		this.executionGraph = checkNotNull(executionGraph);
	}

	// ------------------------------------------------------------------------

	@Override
	public void onTaskFailure(Execution taskExecution, Throwable cause) {
		// this strategy makes every task failure a global failure
		executionGraph.failGlobal(cause);
	}

	@Override
	public void notifyNewVertices(List<ExecutionJobVertex> newJobVerticesTopological) {
		// nothing to do
	}

	@Override
	public String getStrategyName() {
		return "full graph restart";
	}

	// ------------------------------------------------------------------------
	//  factory
	// ------------------------------------------------------------------------

	/**
	 * Factory that instantiates the RestartAllStrategy.
	 */
	public static class Factory implements FailoverStrategy.Factory {

		@Override
		public FailoverStrategy create(ExecutionGraph executionGraph) {
			return new RestartAllStrategy(executionGraph);
		}
	}
}
