

package org.apache.flink.table.runtime.arrow;

import org.apache.flink.annotation.Internal;

/**
 * Reader which deserialize the Arrow format data to the Flink rows.
 *
 * @param <OUT> Type of the deserialized row.
 */
@Internal
public interface ArrowReader<OUT> {

	/**
	 * Read the specified row from underlying Arrow format data.
	 */
	OUT read(int rowId);
}
