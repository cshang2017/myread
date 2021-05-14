package org.apache.flink.table.runtime.arrow;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.runtime.arrow.writers.ArrowFieldWriter;
import org.apache.flink.util.Preconditions;

import org.apache.arrow.vector.VectorSchemaRoot;

/**
 * Writer which serializes the Flink rows to Arrow format.
 *
 * @param <IN> Type of the row to write.
 */
@Internal
public final class ArrowWriter<IN> {

	/**
	 * Container that holds a set of vectors for the rows to be sent to the Python worker.
	 */
	private final VectorSchemaRoot root;

	/**
	 * An array of writers which are responsible for the serialization of each column of the rows.
	 */
	private final ArrowFieldWriter<IN>[] fieldWriters;

	public ArrowWriter(VectorSchemaRoot root, ArrowFieldWriter<IN>[] fieldWriters) {
		this.root = Preconditions.checkNotNull(root);
		this.fieldWriters = Preconditions.checkNotNull(fieldWriters);
	}

	/**
	 * Gets the field writers.
	 */
	public ArrowFieldWriter<IN>[] getFieldWriters() {
		return fieldWriters;
	}

	/**
	 * Writes the specified row which is serialized into Arrow format.
	 */
	public void write(IN row) {
		for (int i = 0; i < fieldWriters.length; i++) {
			fieldWriters[i].write(row, i);
		}
	}

	/**
	 * Finishes the writing of the current row batch.
	 */
	public void finish() {
		root.setRowCount(fieldWriters[0].getCount());
		for (ArrowFieldWriter fieldWriter : fieldWriters) {
			fieldWriter.finish();
		}
	}

	/**
	 * Resets the state of the writer to write the next batch of rows.
	 */
	public void reset() {
		root.setRowCount(0);
		for (ArrowFieldWriter fieldWriter : fieldWriters) {
			fieldWriter.reset();
		}
	}
}
