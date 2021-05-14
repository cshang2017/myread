package org.apache.flink.runtime.jobgraph.tasks;

import org.apache.flink.annotation.Public;
import org.apache.flink.core.io.InputSplit;

/**
 * An input split provider can be successively queried to provide a series of {@link InputSplit} objects a
 * task is supposed to consume in the course of its execution.
 */
@Public
public interface InputSplitProvider {

	/**
	 * Requests the next input split to be consumed by the calling task.
	 *
	 * @param userCodeClassLoader used to deserialize input splits
	 * @return the next input split to be consumed by the calling task or <code>null</code> if the
	 *         task shall not consume any further input splits.
	 * @throws InputSplitProviderException if fetching the next input split fails
	 */
	InputSplit getNextInputSplit(ClassLoader userCodeClassLoader) throws InputSplitProviderException;
}
