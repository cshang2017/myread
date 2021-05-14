package org.apache.flink.table.runtime.hashtable;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.util.RowIterator;
import org.apache.flink.util.MutableObjectIterator;

import java.io.IOException;

/**
 * Wrap {@link MutableObjectIterator} to java {@link RowIterator}.
 */
public class WrappedRowIterator<T extends RowData> implements RowIterator<T> {

	private final MutableObjectIterator<T> iterator;
	private final T reuse;
	private T instance;

	public WrappedRowIterator(MutableObjectIterator<T> iterator, T reuse) {
		this.iterator = iterator;
		this.reuse = reuse;
	}

	@Override
	public boolean advanceNext() {
		try {
			this.instance = iterator.next(reuse);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		return this.instance != null;
	}

	@Override
	public T getRow() {
		return instance;
	}
}
