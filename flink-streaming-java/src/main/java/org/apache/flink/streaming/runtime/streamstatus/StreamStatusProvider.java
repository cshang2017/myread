package org.apache.flink.streaming.runtime.streamstatus;

import org.apache.flink.annotation.Internal;

/**
 * Interface for retrieving the current {@link StreamStatus}.
 */
@Internal
public interface StreamStatusProvider {

	/**
	 * Returns the current stream status.
	 *
	 * @return current stream status.
	 */
	StreamStatus getStreamStatus();
}
