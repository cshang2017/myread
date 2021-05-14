package org.apache.flink.table.runtime.operators.aggregate;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.table.runtime.util.MemorySegmentPool;

import java.util.List;

/**
 * MemorySegmentPool for {@link BytesHashMap} to fall back to sort agg.
 * {@link #nextSegment} not remove segment from segments, just get from segments.
 */
public class BytesHashMapSpillMemorySegmentPool implements MemorySegmentPool {

	private final List<MemorySegment> segments;
	private final int pageSize;
	private int allocated;

	public BytesHashMapSpillMemorySegmentPool(List<MemorySegment> memorySegments) {
		this.segments = memorySegments;
		this.pageSize = memorySegments.get(0).size();
		this.allocated = 0;
	}

	@Override
	public MemorySegment nextSegment() {
		allocated++;
		if (allocated <= segments.size()) {
			return segments.get(allocated - 1);
		} else {
			return MemorySegmentFactory.wrap(new byte[pageSize()]);
		}
	}

	@Override
	public void returnAll(List<MemorySegment> memory) {
		throw new UnsupportedOperationException("not support!");
	}

	@Override
	public int freePages() {
		return Integer.MAX_VALUE;
	}

	@Override
	public int pageSize() {
		return pageSize;
	}
}
