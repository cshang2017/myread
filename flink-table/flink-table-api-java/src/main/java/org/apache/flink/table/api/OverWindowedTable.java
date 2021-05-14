package org.apache.flink.table.api;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.expressions.Expression;

/**
 * A table that has been windowed for {@link OverWindow}s.
 *
 * <p>Unlike group windows, which are specified in the
 * {@code GROUP BY} clause, over windows do not collapse rows. Instead over window aggregates
 * compute an aggregate for each input row over a range of its neighboring rows.
 */
@PublicEvolving
public interface OverWindowedTable {

	/**
	 * Performs a selection operation on a over windowed table. Similar to an SQL SELECT statement.
	 * The field expressions can contain complex expressions and aggregations.
	 *
	 * <p>Example:
	 *
	 * <pre>
	 * {@code
	 *   overWindowedTable.select("c, b.count over ow, e.sum over ow")
	 * }
	 * </pre>
	 * @deprecated use {@link #select(Expression...)}
	 */
	@Deprecated
	Table select(String fields);

	/**
	 * Performs a selection operation on a over windowed table. Similar to an SQL SELECT statement.
	 * The field expressions can contain complex expressions and aggregations.
	 *
	 * <p>Example:
	 *
	 * <pre>
	 * {@code
	 *   overWindowedTable.select(
	 *      $("c"),
	 *      $("b").count().over($("ow")),
	 *      $("e").sum().over($("ow"))
	 *   );
	 * }
	 * </pre>
	 *
	 * <p>Scala Example:
	 *
	 * <pre>
	 * {@code
	 *   overWindowedTable.select('c, 'b.count over 'ow, 'e.sum over 'ow)
	 * }
	 * </pre>
	 */
	Table select(Expression... fields);
}
