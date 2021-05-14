package org.apache.flink.table.runtime.operators.sort;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.table.runtime.util.MemorySegmentPool;

import java.util.List;

/**
 * MemorySegment pool of a MemorySegment list.
 */
public class ListMemorySegmentPool implements MemorySegmentPool {

	private final List<MemorySegment> segments;
	private final int pageSize;

	public ListMemorySegmentPool(List<MemorySegment> memorySegments) {
		this.segments = memorySegments;
		this.pageSize = segments.get(0).size();
	}

	@Override
	public MemorySegment nextSegment() {
		if (this.segments.size() > 0) {
			return this.segments.remove(this.segments.size() - 1);
		} else {
			return null;
		}
	}

	@Override
	public int pageSize() {
		return pageSize;
	}

	@Override
	public void returnAll(List<MemorySegment> memory) {
		segments.addAll(memory);
	}

	@Override
	public int freePages() {
		return segments.size();
	}

	public void clear() {
		segments.clear();
	}
}
