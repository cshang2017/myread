package org.apache.flink.runtime.jobmaster.factories;

import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.metrics.groups.JobManagerJobMetricGroup;

import javax.annotation.Nonnull;

/**
 * Factory interface for {@link JobManagerJobMetricGroup}.
 */
public interface JobManagerJobMetricGroupFactory {

	/**
	 * Create a new {@link JobManagerJobMetricGroup}.
	 *
	 * @param jobGraph for which to create a new {@link JobManagerJobMetricGroup}.
	 * @return {@link JobManagerJobMetricGroup}
	 */
	JobManagerJobMetricGroup create(@Nonnull JobGraph jobGraph);
}
