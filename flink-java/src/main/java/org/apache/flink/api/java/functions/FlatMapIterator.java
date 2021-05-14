

package org.apache.flink.api.java.functions;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.util.Collector;

import java.util.Iterator;

/**
 * A convenience variant of the {@link org.apache.flink.api.common.functions.RichFlatMapFunction} that returns elements through an iterator, rather then
 * through a collector. In all other respects, it behaves exactly like the FlatMapFunction.
 *
 * <p>The function needs to be serializable, as defined in {@link java.io.Serializable}.
 *
 * @param <IN> Type of the input elements.
 * @param <OUT> Type of the returned elements.
 */
@PublicEvolving
public abstract class FlatMapIterator<IN, OUT> extends RichFlatMapFunction<IN, OUT> {

	/**
	 * The core method of the function. Takes an element from the input data set and transforms
	 * it into zero, one, or more elements.
	 *
	 * @param value The input value.
	 * @return An iterator over the returned elements.
	 *
	 * @throws Exception This method may throw exceptions. Throwing an exception will cause the operation
	 *                   to fail and may trigger recovery.
	 */
	public abstract Iterator<OUT> flatMap(IN value) throws Exception;

	// --------------------------------------------------------------------------------------------

	/**
	 * Delegates calls to the {@link #flatMap(Object)} method.
	 */
	@Override
	public final void flatMap(IN value, Collector<OUT> out) throws Exception {
		for (Iterator<OUT> iter = flatMap(value); iter.hasNext(); ) {
			out.collect(iter.next());
		}
	}
}
