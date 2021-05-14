package org.apache.flink.streaming.runtime.partitioner;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.runtime.plugable.SerializationDelegate;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.Preconditions;

/**
 * Partitioner selects the target channel based on the key group index.
 *
 * @param <T> Type of the elements in the Stream being partitioned
 */
@Internal
public class KeyGroupStreamPartitioner<T, K> extends StreamPartitioner<T> implements ConfigurableStreamPartitioner {
	private static final long serialVersionUID = 1L;

	private final KeySelector<T, K> keySelector;

	private int maxParallelism;

	public KeyGroupStreamPartitioner(KeySelector<T, K> keySelector, int maxParallelism) {
		Preconditions.checkArgument(maxParallelism > 0, "Number of key-groups must be > 0!");
		this.keySelector = Preconditions.checkNotNull(keySelector);
		this.maxParallelism = maxParallelism;
	}

	public int getMaxParallelism() {
		return maxParallelism;
	}

	@Override
	public int selectChannel(SerializationDelegate<StreamRecord<T>> record) {
		K key;
		try {
			key = keySelector.getKey(record.getInstance().getValue());
		} catch (Exception e) {
			throw new RuntimeException("Could not extract key from " + record.getInstance().getValue(), e);
		}
		return KeyGroupRangeAssignment.assignKeyToParallelOperator(key, maxParallelism, numberOfChannels);
	}

	@Override
	public StreamPartitioner<T> copy() {
		return this;
	}

	@Override
	public String toString() {
		return "HASH";
	}

	@Override
	public void configure(int maxParallelism) {
		KeyGroupRangeAssignment.checkParallelismPreconditions(maxParallelism);
		this.maxParallelism = maxParallelism;
	}
}
