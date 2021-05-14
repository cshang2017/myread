package org.apache.flink.table.data.vector.writable;

import org.apache.flink.table.data.vector.ColumnVector;
import org.apache.flink.table.data.vector.Dictionary;

import java.io.Serializable;

/**
 * Contains the shared structure for {@link ColumnVector}s, including NULL information and dictionary.
 * NOTE: if there are some nulls, must set {@link #noNulls} to false.
 */
public abstract class AbstractWritableVector implements WritableColumnVector, Serializable {

	private static final long serialVersionUID = 1L;

	// If the whole column vector has no nulls, this is true, otherwise false.
	protected boolean noNulls = true;

	/**
	 * The Dictionary for this column.
	 * If it's not null, will be used to decode the value in get().
	 */
	protected Dictionary dictionary;

	/**
	 * Update the dictionary.
	 */
	@Override
	public void setDictionary(Dictionary dictionary) {
		this.dictionary = dictionary;
	}

	/**
	 * Returns true if this column has a dictionary.
	 */
	@Override
	public boolean hasDictionary() {
		return this.dictionary != null;
	}
}
