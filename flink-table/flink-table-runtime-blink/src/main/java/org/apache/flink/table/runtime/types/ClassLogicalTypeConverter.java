package org.apache.flink.table.runtime.types;

import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.utils.TypeConversions;

/**
 * Get internal(sql engine execution data formats) and default external class for {@link LogicalType}.
 */
public class ClassLogicalTypeConverter {

	@Deprecated
	public static Class getDefaultExternalClassForType(LogicalType type) {
		return TypeConversions.fromLogicalToDataType(type).getConversionClass();
	}
}
