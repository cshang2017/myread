package org.apache.flink.table.planner.functions.utils;

import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.utils.TypeConversions;

import org.apache.calcite.rel.type.RelDataType;

import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import static org.apache.flink.table.runtime.types.LogicalTypeDataTypeConverter.fromDataTypeToLogicalType;

/**
 * Hack utils for hive function.
 */
public class HiveFunctionUtils {

	public static boolean isHiveFunc(Object function) {
		try {
			getSetArgsMethod(function);
			return true;
		} catch (NoSuchMethodException e) {
			return false;
		}
	}

	private static Method getSetArgsMethod(Object function) throws NoSuchMethodException {
		return function.getClass().getMethod(
				"setArgumentTypesAndConstants", Object[].class, DataType[].class);

	}

	public static Serializable invokeSetArgs(
			Serializable function, Object[] constantArguments, LogicalType[] argTypes) {
		try {
			// See hive HiveFunction
			Method method = getSetArgsMethod(function);
			method.invoke(function, constantArguments, TypeConversions.fromLogicalToDataType(argTypes));
			return function;
		} catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
			throw new RuntimeException(e);
		}
	}

	static RelDataType invokeGetResultType(
			Object function, Object[] constantArguments, LogicalType[] argTypes,
			FlinkTypeFactory typeFactory) {
		try {
			// See hive HiveFunction
			Method method = function.getClass()
					.getMethod("getHiveResultType", Object[].class, DataType[].class);
			DataType resultType = (DataType) method.invoke(
					function, constantArguments, TypeConversions.fromLogicalToDataType(argTypes));
			return typeFactory.createFieldTypeFromLogicalType(fromDataTypeToLogicalType(resultType));
		} catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
			throw new RuntimeException(e);
		}
	}
}
