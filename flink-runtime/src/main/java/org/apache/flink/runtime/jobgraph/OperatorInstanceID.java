package org.apache.flink.runtime.jobgraph;

import java.util.Objects;

/**
 * An ID for physical instance of the operator.
 */
public class OperatorInstanceID  {

	private final int subtaskId;
	private final OperatorID operatorId;

	public static OperatorInstanceID of(int subtaskId, OperatorID operatorID) {
		return new OperatorInstanceID(subtaskId, operatorID);
	}

	public OperatorInstanceID(int subtaskId, OperatorID operatorId) {
		this.subtaskId = subtaskId;
		this.operatorId = operatorId;
	}

	public int getSubtaskId() {
		return subtaskId;
	}

	public OperatorID getOperatorId() {
		return operatorId;
	}

}
