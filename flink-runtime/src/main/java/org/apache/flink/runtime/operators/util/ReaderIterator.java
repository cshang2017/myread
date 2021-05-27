package org.apache.flink.runtime.operators.util;

import java.io.IOException;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.io.network.api.reader.MutableReader;
import org.apache.flink.runtime.plugable.DeserializationDelegate;
import org.apache.flink.runtime.plugable.NonReusingDeserializationDelegate;
import org.apache.flink.runtime.plugable.ReusingDeserializationDelegate;
import org.apache.flink.util.MutableObjectIterator;

/**
 * A {@link MutableObjectIterator} that wraps a reader from an input channel and
 * produces the reader's records.
 * 
 * The reader supports reading objects with possible reuse of mutable types, and
 * without reuse of mutable types.
 */
public class ReaderIterator<T> implements MutableObjectIterator<T> {
	
	private final MutableReader<DeserializationDelegate<T>> reader;   // the source
	
	private final ReusingDeserializationDelegate<T> reusingDelegate;
	private final NonReusingDeserializationDelegate<T> nonReusingDelegate;

	/**
	 * Creates a new iterator, wrapping the given reader.
	 * 
	 * @param reader The reader to wrap.
	 */
	public ReaderIterator(MutableReader<DeserializationDelegate<T>> reader, TypeSerializer<T> serializer) {
		this.reader = reader;
		this.reusingDelegate = new ReusingDeserializationDelegate<T>(serializer);
		this.nonReusingDelegate = new NonReusingDeserializationDelegate<T>(serializer);
	}

	@Override
	public T next(T target) throws IOException {
		this.reusingDelegate.setInstance(target);
			if (this.reader.next(this.reusingDelegate)) {
				return this.reusingDelegate.getInstance();
			} else {
				return null;
			}
	}

	@Override
	public T next() throws IOException {
			if (this.reader.next(this.nonReusingDelegate)) {
				return this.nonReusingDelegate.getInstance();
			} else {
				return null;
			}
	}
}
