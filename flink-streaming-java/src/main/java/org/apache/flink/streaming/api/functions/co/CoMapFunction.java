package org.apache.flink.streaming.api.functions.co;

import org.apache.flink.annotation.Public;
import org.apache.flink.api.common.functions.Function;

import java.io.Serializable;

/**
 * A CoFlatMapFunction implements a map() transformation over two
 * connected streams.
 *
 * <p>The same instance of the transformation function is used to transform
 * both of the connected streams. That way, the stream transformations can
 * share state.
 *
 * @param <IN1> Type of the first input.
 * @param <IN2> Type of the second input.
 * @param <OUT> Output type.
 */
@Public
public interface CoMapFunction<IN1, IN2, OUT> extends Function, Serializable {

	/**
	 * This method is called for each element in the first of the connected streams.
	 *
	 * @param value The stream element
	 * @return The resulting element
	 * @throws Exception The function may throw exceptions which cause the streaming program
	 *                   to fail and go into recovery.
	 */
	OUT map1(IN1 value) throws Exception;

	/**
	 * This method is called for each element in the second of the connected streams.
	 *
	 * @param value The stream element
	 * @return The resulting element
	 * @throws Exception The function may throw exceptions which cause the streaming program
	 *                   to fail and go into recovery.
	 */
	OUT map2(IN2 value) throws Exception;
}
