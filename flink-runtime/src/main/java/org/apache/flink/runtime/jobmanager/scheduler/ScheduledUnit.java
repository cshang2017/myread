package org.apache.flink.runtime.jobmanager.scheduler;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.instance.SlotSharingGroupId;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

/**
 * ScheduledUnit contains the information necessary to allocate a slot for the given task.
 */
public class ScheduledUnit {

	private final ExecutionVertexID executionVertexId;

	@Nullable
	private final SlotSharingGroupId slotSharingGroupId;

	@Nullable
	private final CoLocationConstraint coLocationConstraint;

	// --------------------------------------------------------------------------------------------

	@VisibleForTesting
	public ScheduledUnit(Execution task) {
		this(
			task,
			null);
	}

	public ScheduledUnit(Execution task, @Nullable SlotSharingGroupId slotSharingGroupId) {
		this(
			task,
			slotSharingGroupId,
			null);
	}

	public ScheduledUnit(
			Execution task,
			@Nullable SlotSharingGroupId slotSharingGroupId,
			@Nullable CoLocationConstraint coLocationConstraint) {
		this(
			Preconditions.checkNotNull(task).getVertex().getID(),
			slotSharingGroupId,
			coLocationConstraint);
	}

	@VisibleForTesting
	public ScheduledUnit(
			JobVertexID jobVertexId,
			@Nullable SlotSharingGroupId slotSharingGroupId,
			@Nullable CoLocationConstraint coLocationConstraint) {
		this(
			new ExecutionVertexID(jobVertexId, 0),
			slotSharingGroupId,
			coLocationConstraint);
	}

	public ScheduledUnit(
			ExecutionVertexID executionVertexId,
			@Nullable SlotSharingGroupId slotSharingGroupId,
			@Nullable CoLocationConstraint coLocationConstraint) {

		this.executionVertexId = Preconditions.checkNotNull(executionVertexId);
		this.slotSharingGroupId = slotSharingGroupId;
		this.coLocationConstraint = coLocationConstraint;

	}

	// --------------------------------------------------------------------------------------------

	public JobVertexID getJobVertexId() {
		return executionVertexId.getJobVertexId();
	}

	public int getSubtaskIndex() {
		return executionVertexId.getSubtaskIndex();
	}

	@Nullable
	public SlotSharingGroupId getSlotSharingGroupId() {
		return slotSharingGroupId;
	}

	@Nullable
	public CoLocationConstraint getCoLocationConstraint() {
		return coLocationConstraint;
	}

	// --------------------------------------------------------------------------------------------

	@Override
	public String toString() {
		return "{task=" + executionVertexId + ", sharingUnit=" + slotSharingGroupId +
				", locationConstraint=" + coLocationConstraint + '}';
	}
}
