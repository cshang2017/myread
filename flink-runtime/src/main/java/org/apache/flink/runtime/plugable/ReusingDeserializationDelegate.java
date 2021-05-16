

package org.apache.flink.runtime.plugable;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;

/**
 * A {@link DeserializationDelegate} that reuses an existing instance upon deserialization.
 */
public class ReusingDeserializationDelegate<T> implements DeserializationDelegate<T> {

	private T instance;

	private final TypeSerializer<T> serializer;

	public ReusingDeserializationDelegate(TypeSerializer<T> serializer) {
		this.serializer = serializer;
	}

	@Override
	public void setInstance(T instance) {
		this.instance = instance;
	}

	@Override
	public T getInstance() {
		return instance;
	}

	@Override
	public void write(DataOutputView out) throws IOException {
		throw new IllegalStateException("Serialization method called on DeserializationDelegate.");
	}

	@Override
	public void read(DataInputView in) throws IOException {
		this.instance = this.serializer.deserialize(this.instance, in);
	}
}
