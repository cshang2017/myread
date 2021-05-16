package org.apache.flink.runtime.executiongraph;

import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.shuffle.ShuffleDescriptor;

import lombok.AllArgsConstructor;
import lombok.Getter;

import java.io.Serializable;

/**
 * Contains information where to find a partition. The partition is defined by the
 * {@link IntermediateDataSetID} and the partition is specified by
 * {@link ShuffleDescriptor}.
 */
@Getter
@AllArgsConstructor
public class PartitionInfo implements Serializable {

	private final IntermediateDataSetID intermediateDataSetID;

	private final ShuffleDescriptor shuffleDescriptor;

}
