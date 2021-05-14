
package org.apache.flink.runtime.minicluster;

import org.apache.flink.util.AutoCloseableAsync;

/**
 * Interface to control {@link JobExecutor}.
 */
public interface JobExecutorService extends JobExecutor, AutoCloseableAsync {
}
