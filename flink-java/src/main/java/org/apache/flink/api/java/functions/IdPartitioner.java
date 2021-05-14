package org.apache.flink.api.java.functions;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.functions.Partitioner;

/**
 * Partitioner that partitions by id.
 */
@Internal
public class IdPartitioner implements Partitioner<Integer> {


	@Override
	public int partition(Integer key, int numPartitions) {
		return key;
	}

}
