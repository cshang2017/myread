package org.apache.flink.runtime.util.config.memory;

import org.apache.flink.configuration.MemorySize;

import java.io.Serializable;

/**
 * Memory components which constitute the Total Flink Memory.
 *
 * <p>The relationships of Flink JVM and rest memory components are shown below.
 * <pre>
 *               ┌ ─ ─  Total Flink Memory - ─ ─ ┐
 *                 ┌───────────────────────────┐
 *               | │       JVM Heap Memory     │ |
 *                 └───────────────────────────┘
 *               |┌ ─ ─ - - - Off-Heap  - - ─ ─ ┐|
 *                │┌───────────────────────────┐│
 *               │ │     JVM Direct Memory     │ │
 *                │└───────────────────────────┘│
 *               │ ┌───────────────────────────┐ │
 *                ││   Rest Off-Heap Memory    ││
 *               │ └───────────────────────────┘ │
 *                └ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ┘
 *               └ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ┘
 * </pre>
 *
 * <p>The JVM and rest memory components can consist of further concrete Flink memory components depending on the process type.
 * The Flink memory components can be derived from either its total size or a subset of configured required fine-grained components.
 * Check the implementations for details about the concrete components.
 */
public interface FlinkMemory extends Serializable {
	MemorySize getJvmHeapMemorySize();

	MemorySize getJvmDirectMemorySize();

	MemorySize getTotalFlinkMemorySize();
}
