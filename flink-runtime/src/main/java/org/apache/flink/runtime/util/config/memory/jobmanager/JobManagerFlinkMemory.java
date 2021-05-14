package org.apache.flink.runtime.util.config.memory.jobmanager;

import org.apache.flink.configuration.MemorySize;
import org.apache.flink.runtime.util.config.memory.FlinkMemory;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Flink internal memory components of Job Manager.
 *
 * <p>A Job Manager's internal Flink memory consists of the following components.
 * <ul>
 *     <li>JVM Heap Memory</li>
 *     <li>Off-Heap Memory (also JVM Direct Memory)</li>
 * </ul>
 *
 * <p>The relationships of Job Manager Flink memory components are shown below.
 * <pre>
 *               ┌ ─ ─  Total Flink Memory - ─ ─ ┐
 *                 ┌───────────────────────────┐
 *               | │       JVM Heap Memory     │ |
 *                 └───────────────────────────┘
 *               │ ┌───────────────────────────┐ │
 *                 |    Off-heap Heap Memory   │   -─ JVM Direct Memory
 *               │ └───────────────────────────┘ │
 *               └ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ┘
 * </pre>
 */
public class JobManagerFlinkMemory implements FlinkMemory {
	private static final long serialVersionUID = 1L;

	private final MemorySize jvmHeap;
	private final MemorySize offHeapMemory;

	JobManagerFlinkMemory(MemorySize jvmHeap, MemorySize offHeapMemory) {
		this.jvmHeap = checkNotNull(jvmHeap);
		this.offHeapMemory = checkNotNull(offHeapMemory);
	}

	@Override
	public MemorySize getJvmHeapMemorySize() {
		return jvmHeap;
	}

	@Override
	public MemorySize getJvmDirectMemorySize() {
		return offHeapMemory;
	}

	@Override
	public MemorySize getTotalFlinkMemorySize() {
		return jvmHeap.add(offHeapMemory);
	}
}
