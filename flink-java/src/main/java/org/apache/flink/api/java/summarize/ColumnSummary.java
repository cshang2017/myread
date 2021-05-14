

package org.apache.flink.api.java.summarize;

import org.apache.flink.annotation.PublicEvolving;

/**
 * Summary for a column of values.
 */
@PublicEvolving
public abstract class ColumnSummary {

	/**
	 * The number of all rows in this column including both nulls and non-nulls.
	 */
	public long getTotalCount() {
		return getNullCount() + getNonNullCount();
	}

	/**
	 * The number of non-null values in this column.
	 */
	public abstract long getNonNullCount();

	/**
	 * The number of null values in this column.
	 */
	public abstract long getNullCount();

	/**
	 * True if this column contains any null values.
	 */
	public boolean containsNull() {
		return getNullCount() > 0L;
	}

	/**
	 * True if this column contains any non-null values.
	 */
	public boolean containsNonNull() {
		return getNonNullCount() > 0L;
	}

}
