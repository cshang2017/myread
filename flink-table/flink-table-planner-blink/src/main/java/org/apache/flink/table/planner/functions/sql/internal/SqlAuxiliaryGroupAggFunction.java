

package org.apache.flink.table.planner.functions.sql.internal;

import org.apache.flink.annotation.Internal;

import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;

/**
 * An internal [[SqlAggFunction]] to represents auxiliary group keys
 * which will not be computed as key and does not also affect the correctness of the final result.
 */
@Internal
public class SqlAuxiliaryGroupAggFunction extends SqlAggFunction {

	public SqlAuxiliaryGroupAggFunction() {
		super("AUXILIARY_GROUP",
				null,
				SqlKind.OTHER_FUNCTION,
				ReturnTypes.ARG0,
				null,
				OperandTypes.ANY,
				SqlFunctionCategory.SYSTEM,
				false,
				false);
	}
}
