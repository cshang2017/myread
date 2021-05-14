package org.apache.flink.runtime.taskexecutor;

import org.apache.flink.runtime.taskexecutor.partition.ClusterPartitionReport;

import java.io.Serializable;

/**
 * Payload for heartbeats sent from the TaskExecutor to the ResourceManager.
 */
public class TaskExecutorHeartbeatPayload implements Serializable {

	private static final long serialVersionUID = -4556838854992435612L;

	private final SlotReport slotReport;
	private final ClusterPartitionReport clusterPartitionReport;

	public TaskExecutorHeartbeatPayload(SlotReport slotReport, ClusterPartitionReport clusterPartitionReport) {
		this.slotReport = slotReport;
		this.clusterPartitionReport = clusterPartitionReport;
	}

	public SlotReport getSlotReport() {
		return slotReport;
	}

	public ClusterPartitionReport getClusterPartitionReport() {
		return clusterPartitionReport;
	}

	@Override
	public String toString() {
		return "TaskExecutorHeartbeatPayload{" +
			"slotReport=" + slotReport +
			", clusterPartitionReport=" + clusterPartitionReport +
			'}';
	}
}
