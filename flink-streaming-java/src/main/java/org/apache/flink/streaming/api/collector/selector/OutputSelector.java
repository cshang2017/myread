package org.apache.flink.streaming.api.collector.selector;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.SplitStream;

import java.io.Serializable;

/**
 * Interface for defining an OutputSelector for a {@link SplitStream} using
 * the {@link SingleOutputStreamOperator#split} call. Every output object of a
 * {@link SplitStream} will run through this operator to select outputs.
 *
 * @param <OUT>
 *            Type parameter of the split values.
 */
@PublicEvolving
public interface OutputSelector<OUT> extends Serializable {
	/**
	 * Method for selecting output names for the emitted objects when using the
	 * {@link SingleOutputStreamOperator#split} method. The values will be
	 * emitted only to output names which are contained in the returned
	 * iterable.
	 *
	 * @param value
	 *            Output object for which the output selection should be made.
	 */
	Iterable<String> select(OUT value);
}
