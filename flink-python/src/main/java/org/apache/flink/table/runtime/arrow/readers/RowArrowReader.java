package org.apache.flink.table.runtime.arrow.readers;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.runtime.arrow.ArrowReader;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

/**
 * {@link ArrowReader} which read the underlying Arrow format data as {@link Row}.
 */
@Internal
public final class RowArrowReader implements ArrowReader<Row> {

	/**
	 * An array of readers which are responsible for the deserialization of each column of the rows.
	 */
	private final ArrowFieldReader[] fieldReaders;

	/**
	 * Reusable row used to hold the deserialized result.
	 */
	private final Row reuseRow;

	public RowArrowReader(ArrowFieldReader[] fieldReaders) {
		this.fieldReaders = Preconditions.checkNotNull(fieldReaders);
		this.reuseRow = new Row(fieldReaders.length);
	}

	/**
	 * Gets the field readers.
	 */
	public ArrowFieldReader[] getFieldReaders() {
		return fieldReaders;
	}

	@Override
	public Row read(int rowId) {
		for (int i = 0; i < fieldReaders.length; i++) {
			reuseRow.setField(i, fieldReaders[i].read(rowId));
		}
		return reuseRow;
	}
}
