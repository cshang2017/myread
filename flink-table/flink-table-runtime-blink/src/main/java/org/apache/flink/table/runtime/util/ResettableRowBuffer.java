package org.apache.flink.table.runtime.util;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryRowData;

import java.io.Closeable;
import java.io.IOException;

/**
 * Resettable buffer that add {@link RowData} and return {@link BinaryRowData} iterator.
 *
 * <p>Instructions:
 * 1.{@link #reset()}
 * 2.multi {@link #add(RowData)}
 * 3.{@link #complete()}
 * 4.multi {@link #newIterator()}
 * repeat the above steps or {@link #close()}.
 */
public interface ResettableRowBuffer extends Closeable {

	/**
	 * Re-initialize the buffer state.
	 */
	void reset();

	/**
	 * Appends the specified row to the end of this buffer.
	 */
	void add(RowData row) throws IOException;

	/**
	 * Finally, complete add.
	 */
	void complete();

	/**
	 * Get a new iterator starting from first row.
	 */
	ResettableIterator newIterator();

	/**
	 * Get a new iterator starting from the `beginRow`-th row. `beginRow` is 0-indexed.
	 */
	ResettableIterator newIterator(int beginRow);

	/**
	 * Row iterator that can be reset.
	 */
	interface ResettableIterator extends RowIterator<BinaryRowData>, Closeable {

		/**
		 * Re-initialize the iterator, start from begin row.
		 */
		void reset() throws IOException;
	}
}
