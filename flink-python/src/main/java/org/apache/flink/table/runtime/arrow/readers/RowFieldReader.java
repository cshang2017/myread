package org.apache.flink.table.runtime.arrow.readers;

import org.apache.flink.annotation.Internal;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import org.apache.arrow.vector.complex.StructVector;

/**
 * {@link ArrowFieldReader} for Row.
 */
@Internal
public final class RowFieldReader extends ArrowFieldReader<Row> {

	private final ArrowFieldReader[] fieldReaders;

	public RowFieldReader(StructVector structVector, ArrowFieldReader[] fieldReaders) {
		super(structVector);
		this.fieldReaders = Preconditions.checkNotNull(fieldReaders);
	}

	@Override
	public Row read(int index) {
		if (getValueVector().isNull(index)) {
			return null;
		} else {
			Row row = new Row(fieldReaders.length);
			for (int i = 0; i < fieldReaders.length; i++) {
				row.setField(i, fieldReaders[i].read(index));
			}
			return row;
		}
	}
}
