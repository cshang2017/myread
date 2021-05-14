package org.apache.flink.api.java.summarize;

import org.apache.flink.annotation.PublicEvolving;

/**
 * Summary for a column of generic Objects (this is a fallback for unsupported types).
 */
@PublicEvolving
public class ObjectColumnSummary extends ColumnSummary {

	private long notNullCount;
	private long nullCount;

	public ObjectColumnSummary(long notNullCount, long nullCount) {
		this.notNullCount = notNullCount;
		this.nullCount = nullCount;
	}

	/**
	 * The number of non-null values in this column.
	 */
	@Override
	public long getNonNullCount() {
		return 0;
	}

	@Override
	public long getNullCount() {
		return nullCount;
	}

	@Override
	public String toString() {
		return "ObjectColumnSummary{" +
			"totalCount=" + getTotalCount() +
			", notNullCount=" + notNullCount +
			", nullCount=" + nullCount +
			'}';
	}
}
