package org.apache.flink.runtime.plugable;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.io.IOReadableWritable;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;

/**
 * The serialization delegate exposes an arbitrary element as a {@link IOReadableWritable} for
 * serialization, with the help of a type serializer.
 *
 * @param <T> The type to be represented as an IOReadableWritable.
 */
public class SerializationDelegate<T> implements IOReadableWritable {

	private T instance;

	private final TypeSerializer<T> serializer;

	public SerializationDelegate(TypeSerializer<T> serializer) {
		this.serializer = serializer;
	}

	public void setInstance(T instance) {
		this.instance = instance;
	}

	public T getInstance() {
		return this.instance;
	}

	@Override
	public void write(DataOutputView out) throws IOException {
		this.serializer.serialize(this.instance, out);
	}

	@Override
	public void read(DataInputView in) throws IOException {
		throw new IllegalStateException("Deserialization method called on SerializationDelegate.");
	}
}
