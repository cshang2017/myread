

package org.apache.flink.api.java.operators.join;

import org.apache.flink.annotation.Public;
import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.operators.JoinOperator;

/**
 * A Join transformation that needs to be finished by specifying either a
 * {@link JoinFunction} or a {@link FlatJoinFunction} before it can be used as an input
 * to other operators.
 *
 * @param <I1> The type of the first input DataSet of the Join transformation.
 * @param <I2> The type of the second input DataSet of the Join transformation.
 */
@Public
public interface JoinFunctionAssigner<I1, I2> {

	<R> JoinOperator<I1, I2, R> with(JoinFunction<I1, I2, R> joinFunction);

	<R> JoinOperator<I1, I2, R> with(FlatJoinFunction<I1, I2, R> joinFunction);

}
