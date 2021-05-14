

package org.apache.flink.table.planner.functions.sql;

import org.apache.flink.shaded.guava18.com.google.common.collect.ImmutableList;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.List;

/**
 * <code>LISTAGG</code> aggregate function returns the concatenation of
 * a list of values that are input to the function.
 *
 * <p>NOTE: The difference between this and {@link SqlStdOperatorTable#LISTAGG} is that:
 * (1). constraint the second parameter must to be a character literal.
 * (2). not require over clause to use this aggregate function.
 */
public class SqlListAggFunction extends SqlAggFunction {

	public SqlListAggFunction() {
		super("LISTAGG",
				null,
				SqlKind.LISTAGG,
				ReturnTypes.ARG0_NULLABLE,
				null,
				OperandTypes.or(
						OperandTypes.CHARACTER,
						OperandTypes.sequence(
								"'LISTAGG(<CHARACTER>, <CHARACTER_LITERAL>)'",
								OperandTypes.CHARACTER,
								OperandTypes.and(OperandTypes.CHARACTER, OperandTypes.LITERAL)
						)),
				SqlFunctionCategory.SYSTEM,
				false,
				false);
	}

	@Override
	public List<RelDataType> getParameterTypes(RelDataTypeFactory typeFactory) {
		return ImmutableList.of(
				typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.VARCHAR), true));
	}

	@Override
	public RelDataType getReturnType(RelDataTypeFactory typeFactory) {
		return typeFactory.createSqlType(SqlTypeName.VARCHAR);
	}
}
