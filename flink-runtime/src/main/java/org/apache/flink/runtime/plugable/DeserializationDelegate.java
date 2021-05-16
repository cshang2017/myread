package org.apache.flink.runtime.plugable;

import org.apache.flink.core.io.IOReadableWritable;

/**
 * The deserialization delegate is used during deserialization to read an arbitrary element as if it implements
 * {@link IOReadableWritable}, with the help of a type serializer.
 *
 * @param <T> The type to be represented as an IOReadableWritable.
 */
public interface DeserializationDelegate<T> extends IOReadableWritable {

	void setInstance(T instance);

	T getInstance();
}
