
package org.apache.flink.runtime.rest.util;

import org.apache.flink.configuration.ConfigConstants;

/**
 * This class contains constants to be used by rest components.
 */
public enum RestConstants {
	;

	public static final String REST_CONTENT_TYPE = "application/json; charset=" + ConfigConstants.DEFAULT_CHARSET.name();
	public static final String CONTENT_TYPE_JAR = "application/java-archive";
	public static final String CONTENT_TYPE_BINARY = "application/octet-stream";
}
