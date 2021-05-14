
package org.apache.flink.streaming.api.graph;

import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.streaming.api.transformations.ShuffleMode;
import org.apache.flink.streaming.runtime.partitioner.ForwardPartitioner;
import org.apache.flink.streaming.runtime.partitioner.RescalePartitioner;

/**
 * This mode decides the default {@link ResultPartitionType} of job edges.
 * Note that this only affects job edges which are {@link ShuffleMode#UNDEFINED}.
 */
public enum GlobalDataExchangeMode {
	/** Set all job edges to be {@link ResultPartitionType#BLOCKING}. */
	ALL_EDGES_BLOCKING,

	/**
	 * Set job edges with {@link ForwardPartitioner} to be {@link ResultPartitionType#PIPELINED_BOUNDED}
	 * and other edges to be {@link ResultPartitionType#BLOCKING}.
	 **/
	FORWARD_EDGES_PIPELINED,

	/**
	 * Set job edges with {@link ForwardPartitioner} or {@link RescalePartitioner} to be
	 * {@link ResultPartitionType#PIPELINED_BOUNDED} and other edges to be {@link ResultPartitionType#BLOCKING}.
	 **/
	POINTWISE_EDGES_PIPELINED,

	/** Set all job edges {@link ResultPartitionType#PIPELINED_BOUNDED}. */
	ALL_EDGES_PIPELINED
}
