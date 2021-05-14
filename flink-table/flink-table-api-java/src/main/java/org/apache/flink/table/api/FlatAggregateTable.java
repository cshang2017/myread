package org.apache.flink.table.api;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.expressions.Expression;

/**
 * A table that performs flatAggregate on a {@link Table}, a {@link GroupedTable} or a
 * {@link WindowGroupedTable}.
 */
@PublicEvolving
public interface FlatAggregateTable {

	/**
	 * Performs a selection operation on a FlatAggregateTable. Similar to a SQL SELECT
	 * statement. The field expressions can contain complex expressions.
	 *
	 * <p><b>Note</b>: You have to close the flatAggregate with a select statement. And the select
	 * statement does not support aggregate functions.
	 *
	 * <p>Example:
	 *
	 * <pre>
	 * {@code
	 *   TableAggregateFunction tableAggFunc = new MyTableAggregateFunction();
	 *   tableEnv.registerFunction("tableAggFunc", tableAggFunc);
	 *   tab.groupBy("key")
	 *     .flatAggregate("tableAggFunc(a, b) as (x, y, z)")
	 *     .select("key, x, y, z")
	 * }
	 * </pre>
	 * @deprecated use {@link #select(Expression...)}
	 */
	@Deprecated
	Table select(String fields);

	/**
	 * Performs a selection operation on a FlatAggregateTable table. Similar to a SQL SELECT
	 * statement. The field expressions can contain complex expressions.
	 *
	 * <p><b>Note</b>: You have to close the flatAggregate with a select statement. And the select
	 * statement does not support aggregate functions.
	 *
	 * <p>Example:
	 *
	 * <pre>
	 * {@code
	 *   TableAggregateFunction tableAggFunc = new MyTableAggregateFunction();
	 *   tableEnv.registerFunction("tableAggFunc", tableAggFunc);
	 *   tab.groupBy($("key"))
	 *     .flatAggregate(call("tableAggFunc", $("a"), $("b")).as("x", "y", "z"))
	 *     .select($("key"), $("x"), $("y"), $("z"));
	 * }
	 * </pre>
	 *
	 * <p>Scala Example:
	 *
	 * <pre>
	 * {@code
	 *   val tableAggFunc: TableAggregateFunction = new MyTableAggregateFunction
	 *   tab.groupBy($"key")
	 *     .flatAggregate(tableAggFunc($"a", $"b") as ("x", "y", "z"))
	 *     .select($"key", $"x", $"y", $"z")
	 * }
	 * </pre>
	 */
	Table select(Expression... fields);
}
