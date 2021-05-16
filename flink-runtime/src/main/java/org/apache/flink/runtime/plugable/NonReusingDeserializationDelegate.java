

package org.apache.flink.runtime.plugable;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;

/**
 * A {@link DeserializationDelegate} that always creates a new instance upon deserialization.
 */
public class NonReusingDeserializationDelegate<T> implements DeserializationDelegate<T> {

	private T instance;

	private final TypeSerializer<T> serializer;

	public NonReusingDeserializationDelegate(TypeSerializer<T> serializer) {
		this.serializer = serializer;
	}

	public void setInstance(T instance) {
		this.instance = instance;
	}

	public T getInstance() {
		return instance;
	}

	@Override
	public void write(DataOutputView out) throws IOException {
		throw new IllegalStateException("Serialization method called on DeserializationDelegate.");
	}

	@Override
	public void read(DataInputView in) throws IOException {
		this.instance = this.serializer.deserialize(in);
	}
}
