package org.apache.flink.streaming.runtime.partitioner;

import org.apache.flink.annotation.Internal;
import org.apache.flink.runtime.plugable.SerializationDelegate;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

/**
 * Partitioner that forwards elements only to the locally running downstream operation.
 *
 * @param <T> Type of the elements in the Stream
 */
@Internal
public class ForwardPartitioner<T> extends StreamPartitioner<T> {

	@Override
	public int selectChannel(SerializationDelegate<StreamRecord<T>> record) {
		return 0;
	}

	public StreamPartitioner<T> copy() {
		return this;
	}

	@Override
	public String toString() {
		return "FORWARD";
	}
}
