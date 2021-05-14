package org.apache.flink.table.runtime.util;

import org.apache.flink.table.data.RowData;

import java.util.Iterator;

/**
 * An internal iterator interface which presents a more restrictive API than {@link Iterator}.
 *
 * <p>One major departure from the Java iterator API is the fusing of the `hasNext()` and `next()`
 * calls: Java's iterator allows users to call `hasNext()` without immediately advancing the
 * iterator to consume the next row, whereas {@link RowIterator} combines these calls into a single
 * {@link #advanceNext()} method.
 */
public interface RowIterator<T extends RowData> {
	/**
	 * Advance this iterator by a single row. Returns `false` if this iterator has no more rows
	 * and `true` otherwise. If this returns `true`, then the new row can be retrieved by calling
	 * {@link #getRow()}.
		*/
	boolean advanceNext();

	/**
	 * Retrieve the row from this iterator. This method is idempotent. It is illegal to call this
	 * method after [[advanceNext()]] has returned `false`.
	 */
	T getRow();
}
