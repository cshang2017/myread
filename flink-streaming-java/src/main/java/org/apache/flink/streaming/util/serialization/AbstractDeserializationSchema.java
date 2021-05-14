package org.apache.flink.streaming.util.serialization;

import org.apache.flink.annotation.PublicEvolving;

/**
 * The deserialization schema describes how to turn the byte messages delivered by certain
 * data sources (for example Apache Kafka) into data types (Java/Scala objects) that are
 * processed by Flink.
 *
 * <p>This base variant of the deserialization schema produces the type information
 * automatically by extracting it from the generic class arguments.
 *
 * @param <T> The type created by the deserialization schema.
 *
 * @deprecated Use {@link org.apache.flink.api.common.serialization.AbstractDeserializationSchema} instead.
 */
@Deprecated
@PublicEvolving
@SuppressWarnings("deprecation")
public abstract class AbstractDeserializationSchema<T>
		extends org.apache.flink.api.common.serialization.AbstractDeserializationSchema<T>
		implements DeserializationSchema<T> {

}
