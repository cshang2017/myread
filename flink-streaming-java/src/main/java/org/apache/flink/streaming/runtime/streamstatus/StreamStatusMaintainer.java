package org.apache.flink.streaming.runtime.streamstatus;

import org.apache.flink.annotation.Internal;

/**
 * Interface that allows toggling the current {@link StreamStatus} as well as retrieving it.
 */
@Internal
public interface StreamStatusMaintainer extends StreamStatusProvider {

	/**
	 * Toggles the current stream status. This method should only have effect
	 * if the supplied stream status is different from the current status.
	 *
	 * @param streamStatus the new status to toggle to
	 */
	void toggleStreamStatus(StreamStatus streamStatus);

}
