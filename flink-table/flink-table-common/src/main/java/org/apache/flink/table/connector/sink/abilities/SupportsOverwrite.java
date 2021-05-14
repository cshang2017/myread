

package org.apache.flink.table.connector.sink.abilities;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.connector.sink.DynamicTableSink;

/**
 * Enables to overwrite existing data in a {@link DynamicTableSink}.
 *
 * <p>By default, if this interface is not implemented, existing tables or partitions cannot be overwritten
 * using e.g. the SQL {@code INSERT OVERWRITE} clause.
 */
@PublicEvolving
public interface SupportsOverwrite {

	/**
	 * Provides whether existing data should be overwritten or not.
	 */
	void applyOverwrite(boolean overwrite);
}
