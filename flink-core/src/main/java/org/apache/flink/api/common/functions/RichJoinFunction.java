package org.apache.flink.api.common.functions;

import org.apache.flink.annotation.Public;

/**
 * Rich variant of the {@link JoinFunction}. As a {@link RichFunction}, it gives access to the
 * {@link org.apache.flink.api.common.functions.RuntimeContext} and provides setup and teardown methods:
 * {@link RichFunction#open(org.apache.flink.configuration.Configuration)} and
 * {@link RichFunction#close()}.
 *
 * @param <IN1> The type of the elements in the first input.
 * @param <IN2> The type of the elements in the second input.
 * @param <OUT> The type of the result elements.
 */
@Public
public abstract class RichJoinFunction<IN1, IN2, OUT> extends AbstractRichFunction implements JoinFunction<IN1, IN2, OUT> {

	@Override
	public abstract OUT join(IN1 first, IN2 second) throws Exception;
}
