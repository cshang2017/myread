package org.apache.flink.table.api;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.expressions.Expression;

/**
 * A table that has been performed on the aggregate function.
 */
@PublicEvolving
public interface AggregatedTable {

	/**
	 * Performs a selection operation after an aggregate operation. The field expressions
	 * cannot contain table functions and aggregations.
	 *
	 * <p>Example:
	 *
	 * <pre>
	 * {@code
	 *   AggregateFunction aggFunc = new MyAggregateFunction();
	 *   tableEnv.registerFunction("aggFunc", aggFunc);
	 *   table.groupBy("key")
	 *     .aggregate("aggFunc(a, b) as (f0, f1, f2)")
	 *     .select("key, f0, f1");
	 * }
	 * </pre>
	 * @deprecated use {@link #select(Expression...)}
	 */
	@Deprecated
	Table select(String fields);

	/**
	 * Performs a selection operation after an aggregate operation. The field expressions
	 * cannot contain table functions and aggregations.
	 *
	 * <p>Example:
	 *
	 * <pre>
	 * {@code
	 *   AggregateFunction aggFunc = new MyAggregateFunction();
	 *   tableEnv.registerFunction("aggFunc", aggFunc);
	 *   table.groupBy($("key"))
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
	Table select(Expression... fields);
}
