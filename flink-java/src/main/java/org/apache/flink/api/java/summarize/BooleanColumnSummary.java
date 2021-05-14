package org.apache.flink.api.java.summarize;

import org.apache.flink.annotation.PublicEvolving;

/**
 * Summary for a column of booleans.
 */
@PublicEvolving
public class BooleanColumnSummary extends ColumnSummary {

	private long trueCount;
	private long falseCount;
	private long nullCount;

	public BooleanColumnSummary(long trueCount, long falseCount, long nullCount) {
		this.trueCount = trueCount;
		this.falseCount = falseCount;
		this.nullCount = nullCount;
	}

	public long getTrueCount() {
		return trueCount;
	}

	public long getFalseCount() {
		return falseCount;
	}

	/**
	 * The number of non-null values in this column.
	 */
	@Override
	public long getNonNullCount() {
		return trueCount + falseCount;
	}

	public long getNullCount() {
		return nullCount;
	}

	@Override
	public String toString() {
		return "BooleanColumnSummary{" +
			"totalCount=" + getTotalCount() +
			", trueCount=" + trueCount +
			", falseCount=" + falseCount +
			", nullCount=" + nullCount +
			'}';
	}
}
