
package org.apache.flink.streaming.runtime.partitioner;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.runtime.plugable.SerializationDelegate;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

/**
 * Partitioner that selects the channel with a user defined partitioner function on a key.
 *
 * @param <K>
 *            Type of the key
 * @param <T>
 *            Type of the data
 */
@Internal
public class CustomPartitionerWrapper<K, T> extends StreamPartitioner<T> {

	Partitioner<K> partitioner;
	KeySelector<T, K> keySelector;

	public CustomPartitionerWrapper(Partitioner<K> partitioner, KeySelector<T, K> keySelector) {
		this.partitioner = partitioner;
		this.keySelector = keySelector;
	}

	@Override
	public int selectChannel(SerializationDelegate<StreamRecord<T>> record) {
		K key = keySelector.getKey(record.getInstance().getValue());

		return partitioner.partition(key, numberOfChannels);
	}

	@Override
	public StreamPartitioner<T> copy() {
		return this;
	}

	@Override
	public String toString() {
		return "CUSTOM";
	}
}
