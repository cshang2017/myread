package org.apache.flink.streaming.api.operators;

/**
 * A factory to create {@link OneInputStreamOperator}.
 *
 * @param <IN> The input type of the operator.
 * @param <OUT> The output type of the operator.
 */
public interface OneInputStreamOperatorFactory<IN, OUT> extends StreamOperatorFactory<OUT> {
}
