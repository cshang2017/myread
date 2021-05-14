package org.apache.flink.runtime.taskexecutor.partition;

import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.util.Preconditions;

import java.io.Serializable;
import java.util.Collection;
import java.util.Set;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A report about the current status of all cluster partitions of the TaskExecutor, describing
 * which partitions are available.
 */
public class ClusterPartitionReport implements Serializable {

	private final Collection<ClusterPartitionReportEntry> entries;

	public ClusterPartitionReport(final Collection<ClusterPartitionReportEntry> entries) {
		this.entries = checkNotNull(entries);
	}

	public Collection<ClusterPartitionReportEntry> getEntries() {
		return entries;
	}


	/**
	 * An entry describing all partitions belonging to one dataset.
	 */
	public static class ClusterPartitionReportEntry implements Serializable {

		private final IntermediateDataSetID dataSetId;
		private final Set<ResultPartitionID> hostedPartitions;
		private final int numTotalPartitions;

		public ClusterPartitionReportEntry(IntermediateDataSetID dataSetId, Set<ResultPartitionID> hostedPartitions, int numTotalPartitions) {
			Preconditions.checkNotNull(dataSetId);
			Preconditions.checkNotNull(hostedPartitions);
			Preconditions.checkArgument(!hostedPartitions.isEmpty());
			Preconditions.checkArgument(numTotalPartitions > 0);
			Preconditions.checkState(hostedPartitions.size() <= numTotalPartitions);

			this.dataSetId = dataSetId;
			this.hostedPartitions = hostedPartitions;
			this.numTotalPartitions = numTotalPartitions;
		}

		public IntermediateDataSetID getDataSetId() {
			return dataSetId;
		}

		public Set<ResultPartitionID> getHostedPartitions() {
			return hostedPartitions;
		}

		public int getNumTotalPartitions() {
			return numTotalPartitions;
		}
	}
}
