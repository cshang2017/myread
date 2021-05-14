package org.apache.flink.table.sinks;

import org.apache.flink.annotation.Experimental;

/**
 * A {@link TableSink} that supports INSERT OVERWRITE should implement this trait.
 * INSERT OVERWRITE will overwrite any existing data in the table or partition.
 *
 * @see PartitionableTableSink for the definition of partition.
 */
@Experimental
public interface OverwritableTableSink {

	/**
	 * Configures whether the insert should overwrite existing data or not.
	 */
	void setOverwrite(boolean overwrite);
}
