package org.apache.flink.table.planner.calcite;

import org.apache.calcite.rel.type.RelDataType;

/**
 * Factory to create {@link SqlExprToRexConverter}.
 */
public interface SqlExprToRexConverterFactory {

	/**
	 * Creates a new instance of {@link SqlExprToRexConverter} to convert SQL expression
	 * to RexNode.
	 */
	SqlExprToRexConverter create(RelDataType tableRowType);
}
