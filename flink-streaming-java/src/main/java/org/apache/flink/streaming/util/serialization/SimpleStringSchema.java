
package org.apache.flink.streaming.util.serialization;

import org.apache.flink.annotation.PublicEvolving;

import java.nio.charset.Charset;

/**
 * Very simple serialization schema for strings.
 *
 * <p>By default, the serializer uses "UTF-8" for string/byte conversion.
 *
 * @deprecated Use {@link org.apache.flink.api.common.serialization.SimpleStringSchema} instead.
 */
@PublicEvolving
@Deprecated
@SuppressWarnings("deprecation")
public class SimpleStringSchema
		extends org.apache.flink.api.common.serialization.SimpleStringSchema
		implements SerializationSchema<String>, DeserializationSchema<String> {


	public SimpleStringSchema() {
		super();
	}

	/**
	 * Creates a new SimpleStringSchema that uses the given charset to convert between strings and bytes.
	 *
	 * @param charset The charset to use to convert between strings and bytes.
	 */
	public SimpleStringSchema(Charset charset) {
		super(charset);
	}
}
