package org.apache.flink.table.runtime.operators.sort;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.disk.iomanager.AbstractChannelReaderInputView;
import org.apache.flink.util.MutableObjectIterator;

import java.io.EOFException;
import java.io.IOException;
import java.util.List;

/**
 * Key-Value style channel reader input view iterator.
 */
public class ChannelReaderKVInputViewIterator<K, V> implements MutableObjectIterator<Tuple2<K, V>> {
	private final AbstractChannelReaderInputView inView;
	private final TypeSerializer<K> keySerializer;
	private final TypeSerializer<V> valueSerializer;
	private final List<MemorySegment> freeMemTarget;

	public ChannelReaderKVInputViewIterator(
			AbstractChannelReaderInputView inView,
			List<MemorySegment> freeMemTarget,
			TypeSerializer<K> keySerializer,
			TypeSerializer<V> valueSerializer) {
		this.inView = inView;
		this.freeMemTarget = freeMemTarget;
		this.keySerializer = keySerializer;
		this.valueSerializer = valueSerializer;
	}

	@Override
	public Tuple2<K, V> next(Tuple2<K, V> kvPair) throws IOException {
		try {
			kvPair.f0 = this.keySerializer.deserialize(kvPair.f0, this.inView);
			kvPair.f1 = this.valueSerializer.deserialize(kvPair.f1, this.inView);
			return kvPair;
		} catch (EOFException var4) {
			List<MemorySegment> freeMem = this.inView.close();
			if (this.freeMemTarget != null) {
				this.freeMemTarget.addAll(freeMem);
			}
			return null;
		}
	}

	@Override
	public Tuple2<K, V> next() throws IOException {
		throw new UnsupportedOperationException("not supported.");
	}
}
