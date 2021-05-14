

package org.apache.flink.table.planner.functions.sql;

import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;

/**
 * Function to access the timestamp of a StreamRecord.
 */
public class StreamRecordTimestampSqlFunction extends SqlFunction {

	public StreamRecordTimestampSqlFunction() {
		super(
			"STREAMRECORD_TIMESTAMP",
			SqlKind.OTHER_FUNCTION,
			ReturnTypes.explicit(SqlTypeName.BIGINT),
			InferTypes.RETURN_TYPE,
			OperandTypes.family(SqlTypeFamily.NUMERIC),
			SqlFunctionCategory.SYSTEM);
	}

	@Override
	public SqlSyntax getSyntax() {
		return SqlSyntax.FUNCTION;
	}

	@Override
	public boolean isDeterministic() {
		return true;
	}
}
