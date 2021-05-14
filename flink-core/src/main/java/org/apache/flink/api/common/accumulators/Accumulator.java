package org.apache.flink.api.common.accumulators;

import org.apache.flink.annotation.Public;

import java.io.Serializable;

/**
 * Accumulators collect distributed statistics or aggregates in a from user functions
 * and operators. Each parallel instance creates and updates its own accumulator object,
 * and the different parallel instances of the accumulator are later merged.
 * merged by the system at the end of the job. The result can be obtained from the
 * result of a job execution, or from the web runtime monitor.
 *
 * The accumulators are inspired by the Hadoop/MapReduce counters.
 * 
 * The type added to the accumulator might differ from the type returned. This
 * is the case e.g. for a set-accumulator: We add single objects, but the result
 * is a set of objects.
 * 
 * @param <V>
 *            Type of values that are added to the accumulator
 * @param <R>
 *            Type of the accumulator result as it will be reported to the
 *            client
 */
@Public
public interface Accumulator<V, R extends Serializable> extends Serializable, Cloneable {
	/**
	 * @param value
	 *            The value to add to the accumulator object
	 */
	void add(V value);

	/**
	 * @return local The local value from the current UDF context
	 */
	R getLocalValue();

	/**
	 * Reset the local value. This only affects the current UDF context.
	 */
	void resetLocal();

	/**
	 * Used by system internally to merge the collected parts of an accumulator
	 * at the end of the job.
	 * 
	 * @param other Reference to accumulator to merge in.
	 */
	void merge(Accumulator<V, R> other);

	/**
	 * Duplicates the accumulator. All subclasses need to properly implement
	 * cloning and cannot throw a {@link java.lang.CloneNotSupportedException}
	 *
	 * @return The duplicated accumulator.
	 */
	Accumulator<V, R> clone();
}