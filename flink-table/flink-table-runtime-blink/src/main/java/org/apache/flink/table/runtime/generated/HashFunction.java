
package org.apache.flink.table.runtime.generated;

import org.apache.flink.table.data.RowData;

/**
 * Interface for code generated hash code of {@link RowData}, which will select some
 * fields to hash.
 */
public interface HashFunction {

	int hashCode(RowData row);

}
