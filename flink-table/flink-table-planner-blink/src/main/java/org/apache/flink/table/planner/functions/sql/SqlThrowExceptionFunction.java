

package org.apache.flink.table.planner.functions.sql;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.OperandTypes;

/**
 * Function used to throw an exception, only used internally.
 */
public class SqlThrowExceptionFunction extends SqlFunction {
	public SqlThrowExceptionFunction(RelDataType returnType) {
		super(
			"THROW_EXCEPTION",
			SqlKind.OTHER_FUNCTION,
			opBinding -> returnType,
			null,
			OperandTypes.STRING,
			SqlFunctionCategory.USER_DEFINED_FUNCTION);
	}
}
