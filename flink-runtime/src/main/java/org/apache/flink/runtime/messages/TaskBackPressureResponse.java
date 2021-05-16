package org.apache.flink.runtime.messages;

import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.util.Preconditions;

import lombok.AllArgsConstructor;
import lombok.Getter;

import java.io.Serializable;

/**
 * Response to the task back pressure request rpc call.
 */
@Getter
@AllArgsConstructor
public class TaskBackPressureResponse implements Serializable {

	private final int requestId;

	private final ExecutionAttemptID executionAttemptID;

	private final double backPressureRatio;
}
