package org.apache.flink.streaming.api.operators;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

/**
 * {@link Input} interface used in {@link MultipleInputStreamOperator}.
 */
@PublicEvolving
public interface Input<IN> {
	/**
	 * Processes one element that arrived on this input of the {@link MultipleInputStreamOperator}.
	 * This method is guaranteed to not be called concurrently with other methods of the operator.
	 */
	void processElement(StreamRecord<IN> element) throws Exception;

	/**
	 * Processes a {@link Watermark} that arrived on the first input of this two-input operator.
	 * This method is guaranteed to not be called concurrently with other methods of the operator.
	 *
	 * @see org.apache.flink.streaming.api.watermark.Watermark
	 */
	void processWatermark(Watermark mark) throws Exception;

	/**
	 * Processes a {@link LatencyMarker} that arrived on the first input of this two-input operator.
	 * This method is guaranteed to not be called concurrently with other methods of the operator.
	 *
	 * @see org.apache.flink.streaming.runtime.streamrecord.LatencyMarker
	 */
	void processLatencyMarker(LatencyMarker latencyMarker) throws Exception;

	void setKeyContextElement(StreamRecord<IN> record) throws Exception;
}
