

package org.apache.flink.runtime.rest.util;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.SerializationFeature;

/**
 * This class contains utilities for mapping requests and responses to/from JSON.
 */
public class RestMapperUtils {
	private static final ObjectMapper objectMapper;

	static {
		objectMapper = new ObjectMapper();
		objectMapper.enable(
			DeserializationFeature.FAIL_ON_IGNORED_PROPERTIES,
			DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES,
			DeserializationFeature.FAIL_ON_READING_DUP_TREE_KEY);
		objectMapper.disable(
			SerializationFeature.FAIL_ON_EMPTY_BEANS);
	}

	/**
	 * Returns a preconfigured {@link ObjectMapper}.
	 *
	 * @return preconfigured object mapper
	 */
	public static ObjectMapper getStrictObjectMapper() {
		return objectMapper;
	}
}
