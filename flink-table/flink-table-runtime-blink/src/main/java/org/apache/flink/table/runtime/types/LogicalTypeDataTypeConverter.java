package org.apache.flink.table.runtime.types;

import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LegacyTypeInformationType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.utils.TypeConversions;

/**
 * Converter between {@link DataType} and {@link LogicalType}.
 *
 * <p>This class is for:
 * 1.Source, Sink.
 * 2.UDF, UDTF, UDAF.
 * 3.TableEnv.
 */
@Deprecated
public class LogicalTypeDataTypeConverter {

	public static DataType fromLogicalTypeToDataType(LogicalType logicalType) {
		return TypeConversions.fromLogicalToDataType(logicalType);
	}

	/**
	 * It convert {@link LegacyTypeInformationType} to planner types.
	 */
	public static LogicalType fromDataTypeToLogicalType(DataType dataType) {
		return PlannerTypeUtils.removeLegacyTypes(dataType.getLogicalType());
	}
}
