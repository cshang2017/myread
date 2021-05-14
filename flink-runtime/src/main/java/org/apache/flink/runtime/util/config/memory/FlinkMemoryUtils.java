package org.apache.flink.runtime.util.config.memory;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;

/**
 * Utility to derive the {@link FlinkMemory} components.
 *
 * <p>The {@link FlinkMemory} represents memory components which constitute the Total Flink Memory.
 * The Flink memory components can be derived from either its total size or a subset of configured required fine-grained components.
 * See implementations for details about the concrete fine-grained components.
 *
 * @param <FM> the Flink memory components
 */
public interface FlinkMemoryUtils<FM extends FlinkMemory> {
	FM deriveFromRequiredFineGrainedOptions(Configuration config);

	FM deriveFromTotalFlinkMemory(Configuration config, MemorySize totalFlinkMemorySize);
}
