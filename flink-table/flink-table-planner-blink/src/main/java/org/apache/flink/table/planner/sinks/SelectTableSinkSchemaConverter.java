
package org.apache.flink.table.planner.sinks;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.runtime.types.LogicalTypeDataTypeConverter;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.TimestampKind;
import org.apache.flink.table.types.logical.TimestampType;

/**
 * An utility class that provides abilities to change {@link TableSchema}.
 */
public class SelectTableSinkSchemaConverter {

	/**
	 * Change to default conversion class and build a new {@link TableSchema}.
	 */
	public static TableSchema changeDefaultConversionClass(TableSchema tableSchema) {
		DataType[] oldTypes = tableSchema.getFieldDataTypes();
		String[] fieldNames = tableSchema.getFieldNames();

		TableSchema.Builder builder = TableSchema.builder();
		for (int i = 0; i < tableSchema.getFieldCount(); i++) {
			DataType fieldType = LogicalTypeDataTypeConverter.fromLogicalTypeToDataType(
					LogicalTypeDataTypeConverter.fromDataTypeToLogicalType(oldTypes[i]));
			builder.field(fieldNames[i], fieldType);
		}
		return builder.build();
	}

	/**
	 * Convert time attributes (proc time / event time) to regular timestamp
	 * and build a new {@link TableSchema}.
	 */
	public static TableSchema convertTimeAttributeToRegularTimestamp(TableSchema tableSchema) {
		DataType[] dataTypes = tableSchema.getFieldDataTypes();
		String[] oldNames = tableSchema.getFieldNames();

		TableSchema.Builder builder = TableSchema.builder();
		for (int i = 0; i < tableSchema.getFieldCount(); i++) {
			DataType fieldType = dataTypes[i];
			String fieldName = oldNames[i];
			if (fieldType.getLogicalType() instanceof TimestampType) {
				TimestampType timestampType = (TimestampType) fieldType.getLogicalType();
				if (!timestampType.getKind().equals(TimestampKind.REGULAR)) {
					// converts `TIME ATTRIBUTE(ROWTIME)`/`TIME ATTRIBUTE(PROCTIME)` to `TIMESTAMP(3)`
					builder.field(fieldName, DataTypes.TIMESTAMP(3));
					continue;
				}
			}
			builder.field(fieldName, fieldType);
		}
		return builder.build();
	}
}
