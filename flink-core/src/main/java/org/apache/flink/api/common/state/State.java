package org.apache.flink.api.common.state;

import org.apache.flink.annotation.PublicEvolving;

/**
 * Interface that different types of partitioned state must implement.
 *
 * <p>The state is only accessible by functions applied on a {@code KeyedStream}. The key is
 * automatically supplied by the system, so the function always sees the value mapped to the
 * key of the current element. That way, the system can handle stream and state partitioning
 * consistently together.
 */
@PublicEvolving
public interface State {

	/**
	 * Removes the value mapped under the current key.
	 */
	void clear();
}
