package org.apache.flink.table.utils;

import org.apache.flink.annotation.Internal;

/**
 * Utilities for table sources and sinks.
 */
@Internal
public final class TableConnectorUtils {

	private TableConnectorUtils() {
		// do not instantiate
	}

	/**
	 * Returns the table connector name used for logging and web UI.
	 */
	public static String generateRuntimeName(Class<?> clazz, String[] fields) {
		String className = clazz.getSimpleName();
		if (null == fields) {
			return className + "(*)";
		} else {
			return className + "(" + String.join(", ", fields) + ")";
		}
	}
}
