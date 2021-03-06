package org.apache.flink.table.planner.plan.type;

import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.calcite.FlinkTypeSystem;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.VarCharType;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.type.OrdinalReturnTypeInference;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeTransforms;
import org.apache.calcite.sql.type.SqlTypeUtil;

import java.math.BigDecimal;

/**
 * Type inference in Flink.
 */
public class FlinkReturnTypes {

	/**
	 * ROUND(num [,len]) type inference.
	 */
	public static final SqlReturnTypeInference ROUND_FUNCTION = new SqlReturnTypeInference() {
		@Override
		public RelDataType inferReturnType(SqlOperatorBinding opBinding) {
			final RelDataType numType = opBinding.getOperandType(0);
			if (numType.getSqlTypeName() != SqlTypeName.DECIMAL) {
				return numType;
			}
			final BigDecimal lenVal;
			if (opBinding.getOperandCount() == 1) {
				lenVal = BigDecimal.ZERO;
			} else if (opBinding.getOperandCount() == 2) {
				lenVal = getArg1Literal(opBinding); // may return null
			} else {
				throw new AssertionError();
			}
			if (lenVal == null) {
				return numType; //
			}
			// ROUND( decimal(p,s), r )
			final int p = numType.getPrecision();
			final int s = numType.getScale();
			final int r = lenVal.intValueExact();
			DecimalType dt = FlinkTypeSystem.inferRoundType(p, s, r);
			return opBinding.getTypeFactory().createSqlType(
				SqlTypeName.DECIMAL, dt.getPrecision(), dt.getScale());
		}

		private BigDecimal getArg1Literal(SqlOperatorBinding opBinding) {
				return opBinding.getOperandLiteralValue(1, BigDecimal.class);
			
		}
	};

	/**
	 * Type-inference strategy whereby the result type of a call is the type of
	 * the operand #0 (0-based), with nulls always allowed.
	 */
	public static final SqlReturnTypeInference ARG0_VARCHAR_FORCE_NULLABLE = new OrdinalReturnTypeInference(0) {
		@Override
		public RelDataType inferReturnType(SqlOperatorBinding opBinding) {
			RelDataType type = super.inferReturnType(opBinding);
			RelDataType newType;
			switch (type.getSqlTypeName()) {
				case CHAR:
					newType = opBinding.getTypeFactory().createSqlType(SqlTypeName.VARCHAR, type.getPrecision());
					break;
				case VARCHAR:
					newType = type;
					break;
				default:
					throw new UnsupportedOperationException("Unsupported type: " + type);
			}
			return opBinding.getTypeFactory().createTypeWithNullability(newType, true);
		}
	};

	public static final SqlReturnTypeInference FLINK_QUOTIENT_NULLABLE = opBinding -> {
		RelDataType type1 = opBinding.getOperandType(0);
		RelDataType type2 = opBinding.getOperandType(1);
		if (SqlTypeUtil.isDecimal(type1) || SqlTypeUtil.isDecimal(type2)) {
			return ReturnTypes.QUOTIENT_NULLABLE.inferReturnType(opBinding);
		} else {
			RelDataType doubleType = opBinding.getTypeFactory().createSqlType(SqlTypeName.DOUBLE);
			if (type1.isNullable() || type2.isNullable()) {
				return opBinding.getTypeFactory().createTypeWithNullability(doubleType, true);
			} else {
				return doubleType;
			}
		}
	};

	public static final SqlReturnTypeInference FLINK_DIV_NULLABLE = opBinding -> {
		RelDataType type1 = opBinding.getOperandType(0);
		RelDataType type2 = opBinding.getOperandType(1);
		RelDataType returnType;
		if (SqlTypeUtil.isDecimal(type1) || SqlTypeUtil.isDecimal(type2)) {
			DecimalType dt = FlinkTypeSystem.inferIntDivType(
					type1.getPrecision(), type1.getScale(), type2.getScale());
			returnType = opBinding.getTypeFactory().createSqlType(
				SqlTypeName.DECIMAL, dt.getPrecision(), dt.getScale());
		} else { // both are primitive
			returnType = type1;
		}
		return opBinding.getTypeFactory().createTypeWithNullability(returnType,
			type1.isNullable() || type2.isNullable());
	};

	/**
	 * Type-inference strategy that always returns "VARCHAR(2000)" with nulls always allowed.
	 */
	public static final SqlReturnTypeInference VARCHAR_2000_NULLABLE =
		ReturnTypes.cascade(ReturnTypes.VARCHAR_2000, SqlTypeTransforms.FORCE_NULLABLE);

	public static final SqlReturnTypeInference ROUND_FUNCTION_NULLABLE =
		ReturnTypes.cascade(ROUND_FUNCTION, SqlTypeTransforms.TO_NULLABLE);

	public static final SqlReturnTypeInference NUMERIC_FROM_ARG1_DEFAULT1 =
		new NumericOrDefaultReturnTypeInference(1, 1);

	public static final SqlReturnTypeInference NUMERIC_FROM_ARG1_DEFAULT1_NULLABLE =
		ReturnTypes.cascade(NUMERIC_FROM_ARG1_DEFAULT1, SqlTypeTransforms.TO_NULLABLE);

	public static final SqlReturnTypeInference STR_MAP_NULLABLE = ReturnTypes.explicit(
			factory -> ((FlinkTypeFactory) factory).createFieldTypeFromLogicalType(
		new MapType(new VarCharType(VarCharType.MAX_LENGTH), new VarCharType(VarCharType.MAX_LENGTH))));
}
