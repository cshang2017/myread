package org.apache.flink.runtime.executiongraph.metrics;

import org.apache.flink.api.common.JobStatus;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A gauge that returns (in milliseconds) how long a job has been running.
 * 
 * <p>For jobs that are not running any more, it returns {@value NO_LONGER_RUNNING}. 
 */
public class UpTimeGauge implements Gauge<Long> {

	public static final String METRIC_NAME = "uptime";

	private static final long NO_LONGER_RUNNING = -1L;

	// ------------------------------------------------------------------------

	private final ExecutionGraph eg;

	public UpTimeGauge(ExecutionGraph executionGraph) {
		this.eg = checkNotNull(executionGraph);
	}

	// ------------------------------------------------------------------------

	@Override
	public Long getValue() {
		final JobStatus status = eg.getState();

		if (status == JobStatus.RUNNING) {
			// running right now - report the uptime
			final long runningTimestamp = eg.getStatusTimestamp(JobStatus.RUNNING);
			// we use 'Math.max' here to avoid negative timestamps when clocks change
			return Math.max(System.currentTimeMillis() - runningTimestamp, 0);
		}
		else if (status.isTerminalState()) {
			// not running any more -> finished or not on leader
			return NO_LONGER_RUNNING;
		}
		else {
			// not yet running or not up at the moment
			return 0L;
		}
	}
}
