package org.apache.flink.streaming.runtime.partitioner;

import org.apache.flink.annotation.Internal;
import org.apache.flink.runtime.plugable.SerializationDelegate;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

/**
 * Partitioner that sends all elements to the downstream operator with subtask ID=0.
 *
 * @param <T> Type of the elements in the Stream being partitioned
 */
@Internal
public class GlobalPartitioner<T> extends StreamPartitioner<T> {

	@Override
	public int selectChannel(SerializationDelegate<StreamRecord<T>> record) {
		return 0;
	}

	@Override
	public StreamPartitioner<T> copy() {
		return this;
	}

	@Override
	public String toString() {
		return "GLOBAL";
	}
}
