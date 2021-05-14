

package org.apache.flink.table.types.inference;

import org.apache.flink.annotation.PublicEvolving;

import java.util.Optional;

/**
 * Defines the count of accepted arguments (including open intervals) that a function can take.
 *
 * <p>Note: Implementations should implement {@link Object#hashCode()} and {@link Object#equals(Object)}.
 */
@PublicEvolving
public interface ArgumentCount {

	/**
	 * Enables custom validation of argument counts after {@link #getMinCount()} and
	 * {@link #getMaxCount()} have been validated.
	 *
	 * @param count total number of arguments including each argument for a vararg function call
	 */
	boolean isValidCount(int count);

	/**
	 * Returns the minimum number of argument (inclusive) that a function can take.
	 *
	 * <p>{@link Optional#empty()} if such a lower bound is not defined.
	 */
	Optional<Integer> getMinCount();

	/**
	 * Returns the maximum number of argument (inclusive) that a function can take.
	 *
	 * <p>{@link Optional#empty()} if such an upper bound is not defined.
	 */
	Optional<Integer> getMaxCount();
}
