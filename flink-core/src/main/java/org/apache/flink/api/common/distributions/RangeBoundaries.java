
package org.apache.flink.api.common.distributions;

import org.apache.flink.annotation.PublicEvolving;

import java.io.Serializable;

/**
 * RangeBoundaries is used to split the records into multiple ranges.
 *
 * @param <T> The boundary type.
 */
@PublicEvolving
public interface RangeBoundaries<T> extends Serializable {

	/**
	 * Get the range index of record.
	 *
	 * @param record     The input record.
	 * @return The range index.
	 */
	int getRangeIndex(T record);
}
