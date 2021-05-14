package org.apache.flink.runtime.jobmanager;

import org.apache.flink.runtime.io.network.partition.ResultPartitionID;

/**
 * Exception returned to a TaskManager on JobMaster requesting partition state,
 * if the producer of a partition has been disposed.
 */
public class PartitionProducerDisposedException extends Exception {

	public PartitionProducerDisposedException(ResultPartitionID resultPartitionID) {
		super(String.format("Execution %s producing partition %s has already been disposed.",
			resultPartitionID.getProducerId(),
			resultPartitionID.getPartitionId()));
	}

}
