package org.apache.flink.runtime.jobmaster.factories;

import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.metrics.groups.JobManagerJobMetricGroup;
import org.apache.flink.runtime.metrics.groups.JobManagerMetricGroup;

import javax.annotation.Nonnull;

/**
 * Default implementation of {@link JobManagerJobMetricGroupFactory} which creates for a given
 * {@link JobGraph} a {@link JobManagerJobMetricGroup}.
 */
public class DefaultJobManagerJobMetricGroupFactory implements JobManagerJobMetricGroupFactory {

	private final JobManagerMetricGroup jobManagerMetricGroup;

	public DefaultJobManagerJobMetricGroupFactory(@Nonnull JobManagerMetricGroup jobManagerMetricGroup) {
		this.jobManagerMetricGroup = jobManagerMetricGroup;
	}

	@Override
	public JobManagerJobMetricGroup create(@Nonnull JobGraph jobGraph) {
		return jobManagerMetricGroup.addJob(jobGraph);
	}
}
