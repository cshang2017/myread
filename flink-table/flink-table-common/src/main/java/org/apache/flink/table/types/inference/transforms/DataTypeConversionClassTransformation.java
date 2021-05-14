package org.apache.flink.table.types.inference.transforms;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.TypeTransformation;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;

import java.util.Map;

/**
 * This type transformation transforms the specified data types to a new one with the expected
 * conversion class. The mapping from data type to conversion class is defined by the constructor
 * parameter {@link #conversions} map that maps from type root to the expected conversion class.
 */
@Internal
public class DataTypeConversionClassTransformation implements TypeTransformation {

	private final Map<LogicalTypeRoot, Class<?>> conversions;

	public DataTypeConversionClassTransformation(Map<LogicalTypeRoot, Class<?>> conversions) {
		this.conversions = conversions;
	}

	@Override
	public DataType transform(DataType dataType) {
		LogicalType logicalType = dataType.getLogicalType();
		Class<?> conversionClass = conversions.get(logicalType.getTypeRoot());
		if (conversionClass != null) {
			return dataType.bridgedTo(conversionClass);
		} else {
			return dataType;
		}
	}
}
