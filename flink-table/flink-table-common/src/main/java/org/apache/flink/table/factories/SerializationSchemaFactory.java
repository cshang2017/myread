package org.apache.flink.table.factories;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.serialization.SerializationSchema;

import java.util.Map;

/**
 * Factory for creating configured instances of {@link SerializationSchema}.
 *
 * @param <T> record type that the format produces or consumes.
 */
@PublicEvolving
public interface SerializationSchemaFactory<T> extends TableFormatFactory<T> {

	/**
	 * Creates and configures a [[SerializationSchema]] using the given properties.
	 *
	 * @param properties normalized properties describing the format
	 * @return the configured serialization schema or null if the factory cannot provide an
	 *         instance of this class
	 */
	SerializationSchema<T> createSerializationSchema(Map<String, String> properties);

}
