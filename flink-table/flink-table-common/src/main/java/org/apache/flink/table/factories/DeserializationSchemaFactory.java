package org.apache.flink.table.factories;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.serialization.DeserializationSchema;

import java.util.Map;

/**
 * Factory for creating configured instances of {@link DeserializationSchema}.
 *
 * @param <T> record type that the format produces or consumes.
 */
@PublicEvolving
public interface DeserializationSchemaFactory<T> extends TableFormatFactory<T> {

	/**
	 * Creates and configures a {@link DeserializationSchema} using the given properties.
	 *
	 * @param properties normalized properties describing the format
	 * @return the configured serialization schema or null if the factory cannot provide an
	 *         instance of this class
	 */
	DeserializationSchema<T> createDeserializationSchema(Map<String, String> properties);

}
