package org.apache.flink.streaming.api.operators;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.functions.Function;

/**
 * Udf stream operator factory.
 *
 * @param <OUT> The output type of the operator
 */
@Internal
public interface UdfStreamOperatorFactory<OUT> extends StreamOperatorFactory<OUT> {

	/**
	 * @return user define function.
	 */
	Function getUserFunction();

	/**
	 * @return user define function class name.
	 */
	String getUserFunctionClassName();
}
