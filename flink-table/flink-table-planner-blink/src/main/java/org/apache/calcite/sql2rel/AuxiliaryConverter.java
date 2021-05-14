package org.apache.calcite.sql2rel;

/*
 * THIS FILE HAS BEEN COPIED FROM THE APACHE CALCITE PROJECT.
 * We need support extended TUMBLE_ROWTIME and so on in FlinkSqlOperatorTable.
 */

import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;

/** Converts an expression for a group window function (e.g. TUMBLE)
 * into an expression for an auxiliary group function (e.g. TUMBLE_START).
 *
 * @see SqlStdOperatorTable#TUMBLE
 */
public interface AuxiliaryConverter {
	/** Converts an expression.
	 *
	 * @param rexBuilder Rex  builder
	 * @param groupCall Call to the group function, e.g. "TUMBLE($2, 36000)"
	 * @param e Expression holding result of the group function, e.g. "$0"
	 *
	 * @return Expression for auxiliary function, e.g. "$0 + 36000" converts
	 * the result of TUMBLE to the result of TUMBLE_END
	 */
	RexNode convert(RexBuilder rexBuilder, RexNode groupCall, RexNode e);

	/** Simple implementation of {@link AuxiliaryConverter}. */
	class Impl implements AuxiliaryConverter {
		private final SqlFunction f;

		public Impl(SqlFunction f) {
			this.f = f;
		}

		public RexNode convert(RexBuilder rexBuilder, RexNode groupCall,
			RexNode e) {
			return rexBuilder.makeCall(this.f, e);
		}
	}
}

// End AuxiliaryConverter.java
