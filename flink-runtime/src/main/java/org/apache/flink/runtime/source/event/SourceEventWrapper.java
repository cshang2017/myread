package org.apache.flink.runtime.source.event;

import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;

/**
 * A wrapper operator event that contains a custom defined operator event.
 */
@Getter
public class SourceEventWrapper implements OperatorEvent {

	private final SourceEvent sourceEvent;

	public SourceEventWrapper(SourceEvent sourceEvent) {
		this.sourceEvent = sourceEvent;
	}
}
