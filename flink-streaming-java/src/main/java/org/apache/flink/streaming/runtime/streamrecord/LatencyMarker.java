package org.apache.flink.streaming.runtime.streamrecord;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.runtime.jobgraph.OperatorID;

/**
 * Special record type carrying a timestamp of its creation time at a source operator
 * and the vertexId and subtask index of the operator.
 *
 * <p>At sinks, the marker can be used to approximate the time a record needs to travel
 * through the dataflow.
 */
@PublicEvolving
public final class LatencyMarker extends StreamElement {

	// ------------------------------------------------------------------------

	/** The time the latency mark is denoting. */
	private final long markedTime;

	private final OperatorID operatorId;

	private final int subtaskIndex;

	/**
	 * Creates a latency mark with the given timestamp.
	 */
	public LatencyMarker(long markedTime, OperatorID operatorId, int subtaskIndex) {
		this.markedTime = markedTime;
		this.operatorId = operatorId;
		this.subtaskIndex = subtaskIndex;
	}

	/**
	 * Returns the timestamp marked by the LatencyMarker.
	 */
	public long getMarkedTime() {
		return markedTime;
	}

	public OperatorID getOperatorId() {
		return operatorId;
	}

	public int getSubtaskIndex() {
		return subtaskIndex;
	}


	@Override
	public String toString() {
		return "LatencyMarker{" +
				"markedTime=" + markedTime +
				", operatorId=" + operatorId +
				", subtaskIndex=" + subtaskIndex +
				'}';
	}
}
