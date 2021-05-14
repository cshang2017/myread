package org.apache.flink.table.api;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.expressions.Expression;

/**
 * A table that has been windowed for {@link GroupWindow}s.
 */
@PublicEvolving
public interface GroupWindowedTable {

	/**
	 * Groups the elements by a mandatory window and one or more optional grouping attributes.
	 * The window is specified by referring to its alias.
	 *
	 * <p>If no additional grouping attribute is specified and if the input is a streaming table,
	 * the aggregation will be performed by a single task, i.e., with parallelism 1.
	 *
	 * <p>Aggregations are performed per group and defined by a subsequent {@code select(...)}
	 * clause similar to SQL SELECT-GROUP-BY query.
	 *
	 * <p>Example:
	 *
	 * <pre>
	 * {@code
	 *   tab.window([groupWindow].as("w")).groupBy("w, key").select("key, value.avg")
	 * }
	 * </pre>
	 * @deprecated use {@link #groupBy(Expression...)}
	 */
	@Deprecated
	WindowGroupedTable groupBy(String fields);

	/**
	 * Groups the elements by a mandatory window and one or more optional grouping attributes.
	 * The window is specified by referring to its alias.
	 *
	 * <p>If no additional grouping attribute is specified and if the input is a streaming table,
	 * the aggregation will be performed by a single task, i.e., with parallelism 1.
	 *
	 * <p>Aggregations are performed per group and defined by a subsequent {@code select(...)}
	 * clause similar to SQL SELECT-GROUP-BY query.
	 *
	 * <p>Example:
	 *
	 * <pre>
	 * {@code
	 *   tab.window([groupWindow].as("w")).groupBy($("w"), $("key")).select($("key"), $("value").avg());
	 * }
	 * </pre>
	 *
	 * <p>Scala Example:
	 *
	 * <pre>
	 * {@code
	 *   tab.window([groupWindow] as 'w)).groupBy('w, 'key).select('key, 'value.avg)
	 * }
	 * </pre>
	 */
	WindowGroupedTable groupBy(Expression... fields);
}
