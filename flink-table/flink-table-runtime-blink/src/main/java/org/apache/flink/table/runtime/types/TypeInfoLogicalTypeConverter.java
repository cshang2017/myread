package org.apache.flink.table.runtime.types;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.table.runtime.typeutils.BigDecimalTypeInfo;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.utils.TypeConversions;

import static org.apache.flink.table.runtime.types.LogicalTypeDataTypeConverter.fromLogicalTypeToDataType;

/**
 * Converter between {@link TypeInformation} and {@link LogicalType}.
 *
 * <p>This class is for:
 * 1.Source, Sink.
 * 2.UDF, UDTF.
 * 3.Agg, AggFunctions, Expression, DataView.
 */
@Deprecated
public class TypeInfoLogicalTypeConverter {

	/**
	 * It will lose some information. (Like {@link PojoTypeInfo} will converted to {@link RowType})
	 * It and {@link TypeInfoLogicalTypeConverter#fromLogicalTypeToTypeInfo} not allows back-and-forth conversion.
	 */
	public static LogicalType fromTypeInfoToLogicalType(TypeInformation typeInfo) {
		DataType dataType = TypeConversions.fromLegacyInfoToDataType(typeInfo);
		return LogicalTypeDataTypeConverter.fromDataTypeToLogicalType(dataType);
	}

	/**
	 * Use {@link BigDecimalTypeInfo} to retain precision and scale of decimal.
	 */
	public static TypeInformation fromLogicalTypeToTypeInfo(LogicalType type) {
		DataType dataType = fromLogicalTypeToDataType(type)
				.nullable()
				.bridgedTo(ClassLogicalTypeConverter.getDefaultExternalClassForType(type));
		return TypeInfoDataTypeConverter.fromDataTypeToTypeInfo(dataType);
	}
}
