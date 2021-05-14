package org.apache.flink.table.api;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.ExpressionParser;

import java.util.Arrays;

/**
 * Helper class for creating an over window. Similar to SQL, over window aggregates compute an
 * aggregate for each input row over a range of its neighboring rows.
 *
 * <p>Java Example:
 *
 * <pre>
 * {@code
 *    Over.partitionBy("a").orderBy("rowtime").preceding("unbounded_range").as("w")
 * }
 * </pre>
 *
 * <p>Scala Example:
 *
 * <pre>
 * {@code
 *    Over partitionBy 'a orderBy 'rowtime preceding UNBOUNDED_RANGE as 'w
 * }
 * </pre>
 */
@PublicEvolving
public final class Over {

	/**
	 * Partitions the elements on some partition keys.
	 *
	 * <p>Each partition is individually sorted and aggregate functions are applied to each
	 * partition separately.
	 *
	 * @param partitionBy list of field references
	 * @return an over window with defined partitioning
	 */
	public static OverWindowPartitioned partitionBy(String partitionBy) {
		return new OverWindowPartitioned(ExpressionParser.parseExpressionList(partitionBy));
	}

	/**
	 * Partitions the elements on some partition keys.
	 *
	 * <p>Each partition is individually sorted and aggregate functions are applied to each
	 * partition separately.
	 *
	 * @param partitionBy list of field references
	 * @return an over window with defined partitioning
	 */
	public static OverWindowPartitioned partitionBy(Expression... partitionBy) {
		return new OverWindowPartitioned(Arrays.asList(partitionBy));
	}

	/**
	 * Specifies the time attribute on which rows are ordered.
	 *
	 * <p>For streaming tables, reference a rowtime or proctime time attribute here
	 * to specify the time mode.
	 *
	 * <p>For batch tables, refer to a timestamp or long attribute.
	 *
	 * @param orderBy field reference
	 * @return an over window with defined order
	 * @deprecated use {@link #orderBy(Expression)}
	 */
	@Deprecated
	public static OverWindowPartitionedOrdered orderBy(String orderBy) {
		return partitionBy().orderBy(orderBy);
	}

	/**
	 * Specifies the time attribute on which rows are ordered.
	 *
	 * <p>For streaming tables, reference a rowtime or proctime time attribute here
	 * to specify the time mode.
	 *
	 * <p>For batch tables, refer to a timestamp or long attribute.
	 *
	 * @param orderBy field reference
	 * @return an over window with defined order
	 */
	public static OverWindowPartitionedOrdered orderBy(Expression orderBy) {
		return partitionBy().orderBy(orderBy);
	}
}
