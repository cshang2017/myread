package org.apache.flink.table.util;

import org.apache.flink.table.utils.TableConnectorUtils;

/**
 * Utils for table sources and sinks.
 *
 * @deprecated Use {@link TableConnectorUtils} instead.
 */
@Deprecated
public final class TableConnectorUtil {

	/**
	 * Returns the table connector name used for log and web UI.
	 */
	public static String generateRuntimeName(Class<?> clazz, String[] fields) {
		return TableConnectorUtils.generateRuntimeName(clazz, fields);
	}
}
