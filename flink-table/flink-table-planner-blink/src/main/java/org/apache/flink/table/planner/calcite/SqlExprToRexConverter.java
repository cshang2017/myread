package org.apache.flink.table.planner.calcite;

import org.apache.calcite.rex.RexNode;

/**
 * Converts SQL expressions to {@link RexNode}.
 */
public interface SqlExprToRexConverter {

	/**
	 * Converts a SQL expression to a {@link RexNode} expression.
	 *
	 * @param expr a SQL expression which must be quoted and expanded,
	 *             e.g. "`my_catalog`.`my_database`.`my_udf`(`f0`) + 1".
	 */
	RexNode convertToRexNode(String expr);

	/**
	 * Converts an array of SQL expressions to an array of {@link RexNode} expressions.
	 *
	 * @param exprs SQL expressions which must be quoted and expanded,
	 *              e.g. "`my_catalog`.`my_database`.`my_udf`(`f0`) + 1".
	 */
	RexNode[] convertToRexNodes(String[] exprs);

}
