

package org.apache.flink.table.runtime.generated;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.operators.sort.BinaryInMemorySortBuffer;

/**
 * Normalized key computer for {@link BinaryInMemorySortBuffer}.
 * For performance, subclasses are usually implemented through CodeGenerator.
 */
public interface NormalizedKeyComputer {

	/**
	 * Writes a normalized key for the given record into the target {@link MemorySegment}.
	 */
	void putKey(RowData record, MemorySegment target, int offset);

	/**
	 * Compares two normalized keys in respective {@link MemorySegment}.
	 */
	int compareKey(MemorySegment segI, int offsetI, MemorySegment segJ, int offsetJ);

	/**
	 * Swaps two normalized keys in respective {@link MemorySegment}.
	 */
	void swapKey(MemorySegment segI, int offsetI, MemorySegment segJ, int offsetJ);

	/**
	 * Get normalized keys bytes length.
	 */
	int getNumKeyBytes();

	/**
	 * whether the normalized key can fully determines the comparison.
	 */
	boolean isKeyFullyDetermines();

	/**
	 * Flag whether normalized key comparisons should be inverted key.
	 */
	boolean invertKey();
}
