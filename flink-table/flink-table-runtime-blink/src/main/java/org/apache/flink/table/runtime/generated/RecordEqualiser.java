package org.apache.flink.table.runtime.generated;

import org.apache.flink.table.data.RowData;

import java.io.Serializable;

/**
 * Record equaliser for RowData which can compare two RowData and returns whether they are equal.
 */
public interface RecordEqualiser extends Serializable {

	/**
	 * Returns {@code true} if the rows are equal to each other
	 * and {@code false} otherwise.
	 */
	boolean equals(RowData row1, RowData row2);
}
