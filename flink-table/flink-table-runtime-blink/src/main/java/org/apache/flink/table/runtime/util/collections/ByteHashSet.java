package org.apache.flink.table.runtime.util.collections;

/**
 * Byte hash set.
 */
public class ByteHashSet {

	protected boolean containsNull;

	protected boolean[] used;

	public ByteHashSet() {
		used = new boolean[Byte.MAX_VALUE - Byte.MIN_VALUE + 1];
	}

	public boolean add(final byte k) {
		return !used[k - Byte.MIN_VALUE] && (used[k - Byte.MIN_VALUE] = true);
	}

	public void addNull() {
		this.containsNull = true;
	}

	public boolean contains(final byte k) {
		return used[k - Byte.MIN_VALUE];
	}

	public boolean containsNull() {
		return containsNull;
	}

	public void optimize() {
	}
}
