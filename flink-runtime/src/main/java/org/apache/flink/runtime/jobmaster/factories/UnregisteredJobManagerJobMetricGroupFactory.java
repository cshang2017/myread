package org.apache.flink.runtime.jobmaster.factories;

import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.metrics.groups.JobManagerJobMetricGroup;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;

import javax.annotation.Nonnull;

/**
 * {@link JobManagerJobMetricGroupFactory} which returns an unregistered {@link JobManagerJobMetricGroup}.
 */
public enum UnregisteredJobManagerJobMetricGroupFactory implements JobManagerJobMetricGroupFactory {
	INSTANCE;

	@Override
	public JobManagerJobMetricGroup create(@Nonnull JobGraph jobGraph) {
		return UnregisteredMetricGroups.createUnregisteredJobManagerJobMetricGroup();
	}
}
