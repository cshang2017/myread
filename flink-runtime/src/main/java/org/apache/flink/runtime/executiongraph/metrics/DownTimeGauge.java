package org.apache.flink.runtime.executiongraph.metrics;

import org.apache.flink.api.common.JobStatus;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A gauge that returns (in milliseconds) how long a job has not been not running any
 * more, in case it is in a failing/recovering situation. Running jobs return naturally
 * a value of zero.
 * 
 * <p>For jobs that have never run (new not yet scheduled jobs), this gauge returns
 * {@value NOT_YET_RUNNING}, and for jobs that are not running any more, it returns
 * {@value NO_LONGER_RUNNING}. 
 */
public class DownTimeGauge implements Gauge<Long> {

	public static final String METRIC_NAME = "downtime";

	private static final long NOT_YET_RUNNING = 0L;

	private static final long NO_LONGER_RUNNING = -1L;

	// ------------------------------------------------------------------------

	private final ExecutionGraph eg;

	public DownTimeGauge(ExecutionGraph executionGraph) {
		this.eg = checkNotNull(executionGraph);
	}

	// ------------------------------------------------------------------------

	@Override
	public Long getValue() {
		final JobStatus status = eg.getState();

		if (status == JobStatus.RUNNING) {
			// running right now - no downtime
			return 0L;
		}
		else if (status.isTerminalState()) {
			// not running any more -> finished or not on leader
			return NO_LONGER_RUNNING;
		}
		else {
			final long runningTimestamp = eg.getStatusTimestamp(JobStatus.RUNNING);
			if (runningTimestamp > 0) {
				// job was running at some point and is not running now
				// we use 'Math.max' here to avoid negative timestamps when clocks change
				return Math.max(System.currentTimeMillis() - runningTimestamp, 0);
			}
			else {
				// job was never scheduled so far
				return NOT_YET_RUNNING;
			}
		}
	}
}
