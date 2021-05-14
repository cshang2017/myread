
package org.apache.flink.table.sources;

import org.apache.flink.annotation.Experimental;

import java.util.List;
import java.util.Map;

/**
 * An interface for partitionable {@link TableSource}.
 *
 * <p>A {@link PartitionableTableSource} can exclude partitions from reading, which
 * includes skipping the metadata. This is especially useful when there are thousands
 * of partitions in a table.
 *
 * <p>A partition is represented as a {@code Map<String, String>} which maps from partition
 * field name to partition value. Since the map is NOT ordered, the correct order of partition
 * fields should be obtained via partition keys of catalog table.
 */
@Experimental
public interface PartitionableTableSource {

	/**
	 * Returns all the partitions of this {@link PartitionableTableSource}.
	 */
	List<Map<String, String>> getPartitions();

	/**
	 * Applies the remaining partitions to the table source. The {@code remainingPartitions} is
	 * the remaining partitions of {@link #getPartitions()} after partition pruning applied.
	 *
	 * <p>After trying to apply partition pruning, we should return a new {@link TableSource}
	 * instance which holds all pruned-partitions.
	 *
	 * @param remainingPartitions Remaining partitions after partition pruning applied.
	 * @return A new cloned instance of {@link TableSource} holds all pruned-partitions.
	 */
	TableSource applyPartitionPruning(List<Map<String, String>> remainingPartitions);
}
