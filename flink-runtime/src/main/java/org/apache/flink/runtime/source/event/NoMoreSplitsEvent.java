package org.apache.flink.runtime.source.event;

import org.apache.flink.runtime.operators.coordination.OperatorEvent;

/**
 * A source event sent from the SplitEnumerator to the SourceReader to indicate that no more
 * splits will be assigned to the source reader anymore. So once the SplitReader finishes
 * reading the currently assigned splits, they can exit.
 */
public class NoMoreSplitsEvent implements OperatorEvent {


	@Override
	public String toString() {
		return "[NoMoreSplitEvent]";
	}
}
