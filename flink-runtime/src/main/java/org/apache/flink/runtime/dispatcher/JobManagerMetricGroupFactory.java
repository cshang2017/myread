package org.apache.flink.runtime.dispatcher;

import org.apache.flink.runtime.metrics.groups.JobManagerMetricGroup;

/**
 * Factory interface for {@link JobManagerMetricGroup}.
 */
public interface JobManagerMetricGroupFactory {

	/**
	 * @return newly created {@link JobManagerMetricGroup}
	 */
	JobManagerMetricGroup create();
}
