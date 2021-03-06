

package org.apache.flink.table.planner.functions.utils;

import org.apache.flink.table.functions.FunctionIdentifier;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.plan.schema.FlinkTableFunction;
import org.apache.flink.table.runtime.functions.SqlDateTimeUtils;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.Preconditions;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.util.BitString;
import org.apache.calcite.util.DateString;
import org.apache.calcite.util.ImmutableNullableList;
import org.apache.calcite.util.NlsString;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.TimeString;
import org.apache.calcite.util.TimestampString;

import java.io.IOException;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import scala.Tuple3;

import static org.apache.flink.table.planner.functions.utils.HiveFunctionUtils.invokeGetResultType;
import static org.apache.flink.table.planner.functions.utils.HiveFunctionUtils.invokeSetArgs;
import static org.apache.flink.table.planner.functions.utils.UserDefinedFunctionUtils.buildRelDataType;
import static org.apache.flink.table.runtime.types.LogicalTypeDataTypeConverter.fromDataTypeToLogicalType;
import static org.apache.flink.table.runtime.types.LogicalTypeDataTypeConverter.fromLogicalTypeToDataType;

/**
 * Hive {@link TableSqlFunction}.
 * Override getFunction to clone function and invoke {@code HiveGenericUDTF#setArgumentTypesAndConstants}.
 * Override SqlReturnTypeInference to invoke {@code HiveGenericUDTF#getHiveResultType} instead of
 * {@code HiveGenericUDTF#getResultType(Object[], Class[])}.
 *
 * @deprecated TODO hack code, its logical should be integrated to TableSqlFunction
 */
@Deprecated
public class HiveTableSqlFunction extends TableSqlFunction {

	private final TableFunction hiveUdtf;
	private final HiveOperandTypeChecker operandTypeChecker;

	public HiveTableSqlFunction(
			FunctionIdentifier identifier,
			TableFunction hiveUdtf,
			DataType implicitResultType,
			FlinkTypeFactory typeFactory,
			FlinkTableFunction functionImpl,
			HiveOperandTypeChecker operandTypeChecker) {
		super(identifier, identifier.toString(), hiveUdtf, implicitResultType, typeFactory,
				functionImpl, scala.Option.apply(operandTypeChecker));
		this.hiveUdtf = hiveUdtf;
		this.operandTypeChecker = operandTypeChecker;
	}

	@Override
	public TableFunction makeFunction(Object[] constantArguments, LogicalType[] argTypes) {
		TableFunction clone;
		try {
			clone = InstantiationUtil.clone(hiveUdtf);
		} catch (IOException | ClassNotFoundException e) {
			throw new RuntimeException(e);
		}
		return (TableFunction) invokeSetArgs(clone, constantArguments, argTypes);
	}

	@Override
	public RelDataType getRowType(RelDataTypeFactory typeFactory, List<SqlNode> operandList) {
		Preconditions.checkNotNull(operandTypeChecker.previousArgTypes);
		FlinkTypeFactory factory = (FlinkTypeFactory) typeFactory;
		Object[] arguments = convertArguments(
				Arrays.stream(operandTypeChecker.previousArgTypes)
						.map(factory::createFieldTypeFromLogicalType)
						.collect(Collectors.toList()),
				operandList);
		DataType resultType = fromLogicalTypeToDataType(FlinkTypeFactory.toLogicalType(
				invokeGetResultType(hiveUdtf, arguments, operandTypeChecker.previousArgTypes, (FlinkTypeFactory) typeFactory)));
		Tuple3<String[], int[], LogicalType[]> fieldInfo = UserDefinedFunctionUtils.getFieldInfo(resultType);
		return buildRelDataType(typeFactory, fromDataTypeToLogicalType(resultType), fieldInfo._1(), fieldInfo._2());
	}

	/**
	 * This method is copied from calcite, and modify it to not rely on Function.
	 * TODO FlinkTableFunction need implement getElementType.
	 */
	private static Object[] convertArguments(
			List<RelDataType> operandTypes,
			List<SqlNode> operandList) {
		List<Object> arguments = new ArrayList<>(operandList.size());
		// Construct a list of arguments, if they are all constants.
		for (Pair<RelDataType, SqlNode> pair
				: Pair.zip(operandTypes, operandList)) {
			try {
				final Object o = getValue(pair.right);
				final Object o2 = coerce(o, pair.left);
				arguments.add(o2);
			} catch (NonLiteralException e) {
				arguments.add(null);
			}
		}
		return arguments.toArray();
	}

	private static Object coerce(Object value, RelDataType type) {
		if (value == null) {
			return null;
		}
		switch (type.getSqlTypeName()) {
			case CHAR:
				return ((NlsString) value).getValue();
			case BINARY:
				return ((BitString) value).getAsByteArray();
			case DECIMAL:
				return value;
			case BIGINT:
				return ((BigDecimal) value).longValue();
			case INTEGER:
				return ((BigDecimal) value).intValue();
			case SMALLINT:
				return ((BigDecimal) value).shortValue();
			case TINYINT:
				return ((BigDecimal) value).byteValue();
			case DOUBLE:
				return ((BigDecimal) value).doubleValue();
			case REAL:
				return ((BigDecimal) value).floatValue();
			case FLOAT:
				return ((BigDecimal) value).floatValue();
			case DATE:
				return LocalDate.ofEpochDay(((DateString) value).getDaysSinceEpoch());
			case TIME:
				return LocalTime.ofNanoOfDay(((TimeString) value).getMillisOfDay() * 1000_000);
			case TIMESTAMP:
				return SqlDateTimeUtils.unixTimestampToLocalDateTime(((TimestampString) value).getMillisSinceEpoch());
			default:
				throw new RuntimeException("Not support type: " + type);
		}
	}

	private static Object getValue(SqlNode right) throws NonLiteralException {
		switch (right.getKind()) {
			case ARRAY_VALUE_CONSTRUCTOR:
				final List<Object> list = new ArrayList<>();
				for (SqlNode o : ((SqlCall) right).getOperandList()) {
					list.add(getValue(o));
				}
				return ImmutableNullableList.copyOf(list).toArray();
			case MAP_VALUE_CONSTRUCTOR:
				final Map<Object, Object> map = new HashMap<>();
				final List<SqlNode> operands = ((SqlCall) right).getOperandList();
				for (int i = 0; i < operands.size(); i += 2) {
					final SqlNode key = operands.get(i);
					final SqlNode value = operands.get(i + 1);
					map.put(getValue(key), getValue(value));
				}
				return map;
			default:
				if (SqlUtil.isNullLiteral(right, true)) {
					return null;
				}
				if (SqlUtil.isLiteral(right)) {
					return ((SqlLiteral) right).getValue();
				}
				if (right.getKind() == SqlKind.DEFAULT) {
					return null; // currently NULL is the only default value
				}
				throw new NonLiteralException();
		}
	}

	/** Thrown when a non-literal occurs in an argument to a user-defined
	 * table macro. */
	private static class NonLiteralException extends Exception {
	}

	public static HiveOperandTypeChecker operandTypeChecker(
			String name, TableFunction udtf) {
		return new HiveOperandTypeChecker(
				name, udtf, UserDefinedFunctionUtils.checkAndExtractMethods(udtf, "eval"));
	}

	/**
	 * Checker for remember previousArgTypes.
	 *
	 * @deprecated TODO hack code, should modify calcite getRowType to pass operand types
	 */
	@Deprecated
	public static class HiveOperandTypeChecker extends OperandTypeChecker {

		private LogicalType[] previousArgTypes;

		private HiveOperandTypeChecker(
				String name, TableFunction udtf, Method[] methods) {
			super(name, udtf, methods);
		}

		@Override
		public boolean checkOperandTypes(SqlCallBinding callBinding, boolean throwOnFailure) {
			previousArgTypes = UserDefinedFunctionUtils.getOperandTypeArray(callBinding);
			return super.checkOperandTypes(callBinding, throwOnFailure);
		}
	}
}
