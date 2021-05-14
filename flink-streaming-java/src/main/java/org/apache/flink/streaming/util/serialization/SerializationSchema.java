package org.apache.flink.streaming.util.serialization;

import org.apache.flink.annotation.Public;

import java.io.Serializable;

/**
 * The serialization schema describes how to turn a data object into a different serialized
 * representation. Most data sinks (for example Apache Kafka) require the data to be handed
 * to them in a specific format (for example as byte strings).
 *
 * @param <T> The type to be serialized.
 *
 * @deprecated Use {@link org.apache.flink.api.common.serialization.SerializationSchema} instead.
 */
@Public
@Deprecated
public interface SerializationSchema<T>
		extends org.apache.flink.api.common.serialization.SerializationSchema<T>, Serializable {

	/**
	 * Serializes the incoming element to a specified type.
	 *
	 * @param element
	 *            The incoming element to be serialized
	 * @return The serialized element.
	 */
	@Override
	byte[] serialize(T element);
}
