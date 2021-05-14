package org.apache.flink.streaming.util.serialization;

import org.apache.flink.annotation.Public;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;

import java.io.IOException;
import java.io.Serializable;

/**
 * The deserialization schema describes how to turn the byte messages delivered by certain
 * data sources (for example Apache Kafka) into data types (Java/Scala objects) that are
 * processed by Flink.
 *
 * <p>Note: In most cases, one should start from {@link AbstractDeserializationSchema}, which
 * takes care of producing the return type information automatically.
 *
 * @param <T> The type created by the deserialization schema.
 *
 * @deprecated Use {@link org.apache.flink.api.common.serialization.DeserializationSchema} instead.
 */
@Public
@Deprecated
public interface DeserializationSchema<T> extends
		org.apache.flink.api.common.serialization.DeserializationSchema<T>,
		Serializable,
		ResultTypeQueryable<T> {

	/**
	 * Deserializes the byte message.
	 *
	 * @param message The message, as a byte array.
	 *
	 * @return The deserialized message as an object (null if the message cannot be deserialized).
	 */
	@Override
	T deserialize(byte[] message) throws IOException;

	/**
	 * Method to decide whether the element signals the end of the stream. If
	 * true is returned the element won't be emitted.
	 *
	 * @param nextElement The element to test for the end-of-stream signal.
	 * @return True, if the element signals end of stream, false otherwise.
	 */
	@Override
	boolean isEndOfStream(T nextElement);
}
