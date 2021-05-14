package org.apache.flink.api.common.io;

import org.apache.flink.annotation.Public;

import java.io.IOException;

/**
 * This interface may be implemented by {@link OutputFormat}s to have the master finalize them globally.
 * 
 */
@Public
public interface FinalizeOnMaster {

	/**
	 * The method is invoked on the master (JobManager) after all (parallel) instances of an OutputFormat finished.
	 * 
	 * @param parallelism The parallelism with which the format or functions was run.
	 * @throws IOException The finalization may throw exceptions, which may cause the job to abort.
	 */
	void finalizeGlobal(int parallelism) throws IOException;
}
