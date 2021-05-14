package org.apache.flink.streaming.runtime.partitioner;

import org.apache.flink.annotation.Internal;
import org.apache.flink.runtime.plugable.SerializationDelegate;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

/**
 * Partitioner that selects all the output channels.
 *
 * @param <T> Type of the elements in the Stream being broadcast
 */
@Internal
public class BroadcastPartitioner<T> extends StreamPartitioner<T> {

	/**
	 * Note: Broadcast mode could be handled directly for all the output channels
	 * in record writer, so it is no need to select channels via this method.
	 */
	@Override
	public int selectChannel(SerializationDelegate<StreamRecord<T>> record) {
		throw new UnsupportedOperationException("Broadcast partitioner does not support select channels.");
	}

	@Override
	public boolean isBroadcast() {
		return true;
	}

	@Override
	public StreamPartitioner<T> copy() {
		return this;
	}

	@Override
	public String toString() {
		return "BROADCAST";
	}
}
