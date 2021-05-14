package org.apache.flink.runtime.jobgraph.tasks;

import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobmaster.JobMasterOperatorEventGateway;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.util.SerializedValue;

/**
 * Gateway to send an {@link OperatorEvent} from a Task to to the {@link OperatorCoordinator} JobManager side.
 *
 * <p>This is the first step in the chain of sending Operator Events from Operator to Coordinator.
 * Each layer adds further context, so that the inner layers do not need to know about the complete context,
 * which keeps dependencies small and makes testing easier.
 * <pre>
 *     <li>{@code OperatorEventGateway} takes the event, enriches the event with the {@link OperatorID}, and
 *         forwards it to:</li>
 *     <li>{@link TaskOperatorEventGateway} enriches the event with the {@link ExecutionAttemptID} and
 *         forwards it to the:</li>
 *     <li>{@link JobMasterOperatorEventGateway} which is RPC interface from the TaskManager to the JobManager.</li>
 * </pre>
 */
public interface TaskOperatorEventGateway {

	/**
	 * Send an event from the operator (identified by the given operator ID) to the operator
	 * coordinator (identified by the same ID).
	 */
	void sendOperatorEventToCoordinator(OperatorID operator, SerializedValue<OperatorEvent> event);
}
