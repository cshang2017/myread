package org.apache.flink.runtime.jobmanager;

/**
 * Factory for {@link JobGraphStore}.
 */
public interface JobGraphStoreFactory {

	/**
	 * Creates a {@link JobGraphStore}.
	 *
	 * @return a {@link JobGraphStore} instance
	 */
	JobGraphStore create();
}
