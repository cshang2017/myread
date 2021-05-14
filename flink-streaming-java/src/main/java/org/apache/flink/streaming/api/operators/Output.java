package org.apache.flink.streaming.api.operators;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * A {@link org.apache.flink.streaming.api.operators.StreamOperator} is supplied with an object
 * of this interface that can be used to emit elements and other messages, such as barriers
 * and watermarks, from an operator.
 *
 * @param <T> The type of the elements that can be emitted.
 */
@PublicEvolving
public interface Output<T> extends Collector<T> {

	/**
	 * Emits a {@link Watermark} from an operator. This watermark is broadcast to all downstream
	 * operators.
	 *
	 * <p>A watermark specifies that no element with a timestamp lower or equal to the watermark
	 * timestamp will be emitted in the future.
	 */
	void emitWatermark(Watermark mark);

	/**
	 * Emits a record the side output identified by the given {@link OutputTag}.
	 *
	 * @param record The record to collect.
	 */
	<X> void collect(OutputTag<X> outputTag, StreamRecord<X> record);

	void emitLatencyMarker(LatencyMarker latencyMarker);
}
