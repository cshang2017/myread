package org.apache.flink.table.api;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.expressions.Expression;

/**
 * A table that has been grouped on a set of grouping keys.
 */
@PublicEvolving
public interface GroupedTable {

	/**
	 * Performs a selection operation on a grouped table. Similar to an SQL SELECT statement.
	 * The field expressions can contain complex expressions and aggregations.
	 *
	 * <p>Example:
	 *
	 * <pre>
	 * {@code
	 *   tab.groupBy("key").select("key, value.avg + ' The average' as average")
	 * }
	 * </pre>
	 * @deprecated use {@link #select(Expression...)}
	 */
	@Deprecated
	Table select(String fields);

	/**
	 * Performs a selection operation on a grouped table. Similar to an SQL SELECT statement.
	 * The field expressions can contain complex expressions and aggregations.
	 *
	 * <p>Example:
	 *
	 * <pre>
	 * {@code
	 *   tab.groupBy($("key")).select($("key"), $("value").avg().plus(" The average").as("average"));
	 * }
	 * </pre>
	 *
	 * <p>Scala Example:
	 *
	 * <pre>
	 * {@code
	 *   tab.groupBy($"key").select($"key", $"value".avg + " The average" as "average")
	 * }
	 * </pre>
	 */
	Table select(Expression... fields);

	/**
	 * Performs an aggregate operation with an aggregate function. You have to close the
	 * {@link #aggregate(String)} with a select statement. The output will be flattened if the
	 * output type is a composite type.
	 *
	 * <p>Example:
	 *
	 * <pre>
	 * {@code
	 *   AggregateFunction aggFunc = new MyAggregateFunction();
	 *   tableEnv.registerFunction("aggFunc", aggFunc);
	 *   table.groupBy("key")
	 *     .aggregate("aggFunc(a, b) as (f0, f1, f2)")
	 *     .select("key, f0, f1")
	 * }
	 * </pre>
	 * @deprecated use {@link #aggregate(Expression)}
	 */
	@Deprecated
	AggregatedTable aggregate(String aggregateFunction);

	/**
	 * Performs an aggregate operation with an aggregate function. You have to close the
	 * {@link #aggregate(Expression)} with a select statement. The output will be flattened if the
	 * output type is a composite type.
	 *
	 * <p>Example:
	 *
	 * <pre>
	 * {@code
	 *   AggregateFunction aggFunc = new MyAggregateFunction();
	 *   tableEnv.registerFunction("aggFunc", aggFunc);
	 *   tab.groupBy($("key"))
	 *     .aggregate(call("aggFunc", $("a"), $("b")).as("f0", "f1", "f2"))
	 *     .select($("key"), $("f0"), $("f1"));
	 * }
	 * </pre>
	 *
	 * <p>Scala Example:
	 *
	 * <pre>
	 * {@code
	 *   val aggFunc = new MyAggregateFunction
	 *   table.groupBy($"key")
	 *     .aggregate(aggFunc($"a", $"b") as ("f0", "f1", "f2"))
	 *     .select($"key", $"f0", $"f1")
	 * }
	 * </pre>
	 */
	AggregatedTable aggregate(Expression aggregateFunction);

	/**
	 * Performs a flatAggregate operation on a grouped table. FlatAggregate takes a
	 * TableAggregateFunction which returns multiple rows. Use a selection after flatAggregate.
	 *
	 * <p>Example:
	 *
	 * <pre>
	 * {@code
	 *   val tableAggFunc: TableAggregateFunction = new MyTableAggregateFunction
	 *   tableEnv.registerFunction("tableAggFunc", tableAggFunc);
	 *   tab.groupBy("key")
	 *     .flatAggregate("tableAggFunc(a, b) as (x, y, z)")
	 *     .select("key, x, y, z")
	 * }
	 * </pre>
	 * @deprecated use {@link #flatAggregate(Expression)}
	 */
	@Deprecated
	FlatAggregateTable flatAggregate(String tableAggFunction);

	/**
	 * Performs a flatAggregate operation on a grouped table. FlatAggregate takes a
	 * TableAggregateFunction which returns multiple rows. Use a selection after flatAggregate.
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
	FlatAggregateTable flatAggregate(Expression tableAggFunction);
}
