package org.apache.flink.streaming.api.operators;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

/**
 * Interface for stream operators with one input. Use
 * {@link org.apache.flink.streaming.api.operators.AbstractStreamOperator} as a base class if
 * you want to implement a custom operator.
 *
 * @param <IN> The input type of the operator
 * @param <OUT> The output type of the operator
 */
@PublicEvolving
public interface OneInputStreamOperator<IN, OUT> extends StreamOperator<OUT> {

	/**
	 * Processes one element that arrived at this operator.
	 * This method is guaranteed to not be called concurrently with other methods of the operator.
	 */
	void processElement(StreamRecord<IN> element) throws Exception;

	/**
	 * Processes a {@link Watermark}.
	 * This method is guaranteed to not be called concurrently with other methods of the operator.
	 *
	 * @see org.apache.flink.streaming.api.watermark.Watermark
	 */
	void processWatermark(Watermark mark) throws Exception;

	void processLatencyMarker(LatencyMarker latencyMarker) throws Exception;
}
