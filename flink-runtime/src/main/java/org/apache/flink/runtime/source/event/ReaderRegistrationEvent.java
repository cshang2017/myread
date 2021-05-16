
package org.apache.flink.runtime.source.event;

import org.apache.flink.runtime.operators.coordination.OperatorEvent;

/**
 * An {@link OperatorEvent} that registers a {@link org.apache.flink.api.connector.source.SourceReader SourceReader}
 * to the SourceCoordinator.
 */
@Getter
public class ReaderRegistrationEvent implements OperatorEvent {

	private final int subtaskId;
	private final String location;

	public ReaderRegistrationEvent(int subtaskId, String location) {
		this.subtaskId = subtaskId;
		this.location = location;
	}

}
