package org.apache.flink.table.runtime.util;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentSource;

import java.util.List;

/**
 * MemorySegment pool to hold pages in memory.
 */
public interface MemorySegmentPool extends MemorySegmentSource {

	/**
	 * Get the page size of each page this pool holds.
	 *
	 * @return the page size
	 */
	int pageSize();

	/**
	 * Return all pages back into this pool.
	 *
	 * @param memory the pages which want to be returned.
	 */
	void returnAll(List<MemorySegment> memory);

	/**
	 * @return Free page number.
	 */
	int freePages();
}

