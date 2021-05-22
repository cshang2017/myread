package org.apache.flink.runtime.executiongraph.metrics;

import org.apache.flink.api.common.JobStatus;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Gauge which returns the last restarting time.
 * 
 * <p>Restarting time is the time between {@link JobStatus#RESTARTING} and {@link JobStatus#RUNNING},
 * or a terminal state if {@link JobStatus#RUNNING} was not reached.
 * 
 * <p>If the job has not yet reached either of these states, then the time is measured since reaching
 * {@link JobStatus#RESTARTING}. If it is still the initial job execution, then the gauge will return 0.
 */
public class RestartTimeGauge implements Gauge<Long> {

	public static final String METRIC_NAME = "restartingTime";

	// ------------------------------------------------------------------------

	private final ExecutionGraph eg;

	public RestartTimeGauge(ExecutionGraph executionGraph) {
		this.eg = checkNotNull(executionGraph);
	}

	// ------------------------------------------------------------------------

	@Override
	public Long getValue() {
		final JobStatus status = eg.getState();

		final long restartingTimestamp = eg.getStatusTimestamp(JobStatus.RESTARTING);

		final long switchToRunningTimestamp;
		final long lastRestartTime;

		if (restartingTimestamp <= 0) {
			// we haven't yet restarted our job
			return 0L;
		}
		else if ((switchToRunningTimestamp = eg.getStatusTimestamp(JobStatus.RUNNING)) >= restartingTimestamp) {
			// we have transitioned to RUNNING since the last restart
			lastRestartTime = switchToRunningTimestamp - restartingTimestamp;
		}
		else if (status.isTerminalState()) {
			// since the last restart we've switched to a terminal state without touching
			// the RUNNING state (e.g. failing from RESTARTING)
			lastRestartTime = eg.getStatusTimestamp(status) - restartingTimestamp;
		}
		else {
			// we're still somewhere between RESTARTING and RUNNING
			lastRestartTime  = System.currentTimeMillis() - restartingTimestamp;
		}

		// we guard this with 'Math.max' to avoid negative timestamps when clocks re-sync 
		return Math.max(lastRestartTime, 0);
	}
}
