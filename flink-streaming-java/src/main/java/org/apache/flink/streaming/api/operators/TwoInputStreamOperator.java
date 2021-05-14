package org.apache.flink.streaming.api.operators;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

/**
 * Interface for stream operators with two inputs. Use
 * {@link org.apache.flink.streaming.api.operators.AbstractStreamOperator} as a base class if
 * you want to implement a custom operator.
 *
 * @param <IN1> The input type of the operator
 * @param <IN2> The input type of the operator
 * @param <OUT> The output type of the operator
 */
@PublicEvolving
public interface TwoInputStreamOperator<IN1, IN2, OUT> extends StreamOperator<OUT> {

	/**
	 * Processes one element that arrived on the first input of this two-input operator.
	 * This method is guaranteed to not be called concurrently with other methods of the operator.
	 */
	void processElement1(StreamRecord<IN1> element) throws Exception;

	/**
	 * Processes one element that arrived on the second input of this two-input operator.
	 * This method is guaranteed to not be called concurrently with other methods of the operator.
	 */
	void processElement2(StreamRecord<IN2> element) throws Exception;

	/**
	 * Processes a {@link Watermark} that arrived on the first input of this two-input operator.
	 * This method is guaranteed to not be called concurrently with other methods of the operator.
	 *
	 * @see org.apache.flink.streaming.api.watermark.Watermark
	 */
	void processWatermark1(Watermark mark) throws Exception;

	/**
	 * Processes a {@link Watermark} that arrived on the second input of this two-input operator.
	 * This method is guaranteed to not be called concurrently with other methods of the operator.
	 *
	 * @see org.apache.flink.streaming.api.watermark.Watermark
	 */
	void processWatermark2(Watermark mark) throws Exception;

	/**
	 * Processes a {@link LatencyMarker} that arrived on the first input of this two-input operator.
	 * This method is guaranteed to not be called concurrently with other methods of the operator.
	 *
	 * @see org.apache.flink.streaming.runtime.streamrecord.LatencyMarker
	 */
	void processLatencyMarker1(LatencyMarker latencyMarker) throws Exception;

	/**
	 * Processes a {@link LatencyMarker} that arrived on the second input of this two-input operator.
	 * This method is guaranteed to not be called concurrently with other methods of the operator.
	 *
	 * @see org.apache.flink.streaming.runtime.streamrecord.LatencyMarker
	 */
	void processLatencyMarker2(LatencyMarker latencyMarker) throws Exception;

}
