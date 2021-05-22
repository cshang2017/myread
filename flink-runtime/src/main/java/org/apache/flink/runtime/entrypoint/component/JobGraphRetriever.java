package org.apache.flink.runtime.entrypoint.component;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.util.FlinkException;

/**
 * Interface which allows to retrieve the {@link JobGraph}.
 */
public interface JobGraphRetriever {

	/**
	 * Retrieve the {@link JobGraph}.
	 *
	 * @param configuration cluster configuration
	 * @return the retrieved {@link JobGraph}.
	 * @throws FlinkException if the {@link JobGraph} could not be retrieved
	 */
	JobGraph retrieveJobGraph(Configuration configuration) throws FlinkException;
}
