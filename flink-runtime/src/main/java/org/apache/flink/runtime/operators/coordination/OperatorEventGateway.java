package org.apache.flink.runtime.operators.coordination;

import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobgraph.tasks.TaskOperatorEventGateway;
import org.apache.flink.runtime.jobmaster.JobMasterOperatorEventGateway;

/**
 * The gateway through which an Operator can send an {@link OperatorEvent} to the {@link OperatorCoordinator}
 * on the JobManager side.
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
public interface OperatorEventGateway {

	/**
	 * Sends the given event to the coordinator, where it will be handled by the
	 * {@link OperatorCoordinator#handleEventFromOperator(int, OperatorEvent)} method.
	 */
	void sendEventToCoordinator(OperatorEvent event);
}
