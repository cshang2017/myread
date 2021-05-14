package org.apache.flink.table.runtime.arrow.readers;

import org.apache.flink.annotation.Internal;
import org.apache.flink.util.Preconditions;

import org.apache.arrow.vector.ValueVector;

/**
 * Base class for arrow field reader.
 *
 * @param <OUT> Type of the row to write.
 */
@Internal
public abstract class ArrowFieldReader<OUT> {

	/**
	 * Container which is used to store the sequence of values of a column to read.
	 */
	private final ValueVector valueVector;

	public ArrowFieldReader(ValueVector valueVector) {
		this.valueVector = Preconditions.checkNotNull(valueVector);
	}

	/**
	 * Returns the underlying container which stores the sequence of values of a column to read.
	 */
	public ValueVector getValueVector() {
		return valueVector;
	}

	/**
	 * Sets the field value as the specified value.
	 */
	public abstract OUT read(int index);
}
