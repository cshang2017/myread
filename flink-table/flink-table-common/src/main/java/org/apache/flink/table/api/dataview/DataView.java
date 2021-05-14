

package org.apache.flink.table.api.dataview;

import org.apache.flink.annotation.PublicEvolving;

import java.io.Serializable;

/**
 * A {@link DataView} is a collection type that can be used in the accumulator of an
 * {@link org.apache.flink.table.functions.AggregateFunction}.
 *
 * <p>Depending on the context in which the {@code AggregateFunction} is
 * used, a {@link DataView} can be backed by a Java heap collection or a state backend.
 */
@PublicEvolving
public interface DataView extends Serializable {

	/**
	 * Clears the {@link DataView} and removes all data.
	 */
	void clear();
}
