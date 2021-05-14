package org.apache.flink.table.planner.plan.utils;

import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.planner.codegen.CodeGeneratorContext;
import org.apache.flink.table.planner.codegen.ProjectionCodeGenerator;
import org.apache.flink.table.runtime.generated.GeneratedProjection;
import org.apache.flink.table.runtime.keyselector.BinaryRowDataKeySelector;
import org.apache.flink.table.runtime.keyselector.EmptyRowDataKeySelector;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.typeutils.RowDataTypeInfo;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

/**
 * Utility for KeySelector.
 */
public class KeySelectorUtil {

	/**
	 * Create a RowDataKeySelector to extract keys from DataStream which type is RowDataTypeInfo.
	 *
	 * @param keyFields key fields
	 * @param rowType type of DataStream to extract keys
	 * @return the RowDataKeySelector to extract keys from DataStream which type is RowDataTypeInfo.
	 */
	public static RowDataKeySelector getRowDataSelector(int[] keyFields, RowDataTypeInfo rowType) {
		if (keyFields.length > 0) {
			LogicalType[] inputFieldTypes = rowType.getLogicalTypes();
			LogicalType[] keyFieldTypes = new LogicalType[keyFields.length];
			for (int i = 0; i < keyFields.length; ++i) {
				keyFieldTypes[i] = inputFieldTypes[keyFields[i]];
			}
			// do not provide field names for the result key type,
			// because we may have duplicate key fields and the field names may conflict
			RowType returnType = RowType.of(keyFieldTypes);
			RowType inputType = rowType.toRowType();
			GeneratedProjection generatedProjection = ProjectionCodeGenerator.generateProjection(
				CodeGeneratorContext.apply(new TableConfig()),
				"KeyProjection",
				inputType,
				returnType,
				keyFields);
			RowDataTypeInfo keyRowType = RowDataTypeInfo.of(returnType);
			return new BinaryRowDataKeySelector(keyRowType, generatedProjection);
		} else {
			return EmptyRowDataKeySelector.INSTANCE;
		}
	}

}
