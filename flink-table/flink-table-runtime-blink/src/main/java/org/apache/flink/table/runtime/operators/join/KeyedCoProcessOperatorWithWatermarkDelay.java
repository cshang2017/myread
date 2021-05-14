package org.apache.flink.table.runtime.operators.join;

import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.streaming.api.operators.InternalTimeServiceManager;
import org.apache.flink.streaming.api.operators.co.KeyedCoProcessOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.util.Preconditions;

import java.io.Serializable;
import java.util.Optional;
import java.util.function.Consumer;

/**
 * A {@link KeyedCoProcessOperator} that supports holding back watermarks with a static delay.
 */
public class KeyedCoProcessOperatorWithWatermarkDelay<K, IN1, IN2, OUT>
		extends KeyedCoProcessOperator<K, IN1, IN2, OUT> {

	private final Consumer<Watermark> emitter;

	public KeyedCoProcessOperatorWithWatermarkDelay(KeyedCoProcessFunction<K, IN1, IN2, OUT> flatMapper, long watermarkDelay) {
		super(flatMapper);
		Preconditions.checkArgument(watermarkDelay >= 0, "The watermark delay should be non-negative.");
		if (watermarkDelay == 0) {
			// emits watermark without delay
			emitter = (Consumer<Watermark> & Serializable) (Watermark mark) -> output.emitWatermark(mark);
		} else {
			// emits watermark with delay
			emitter = (Consumer<Watermark> & Serializable) (Watermark mark) -> output
					.emitWatermark(new Watermark(mark.getTimestamp() - watermarkDelay));
		}
	}

	@Override
	public void processWatermark(Watermark mark) throws Exception {
		Optional<InternalTimeServiceManager<?>> timeServiceManager = getTimeServiceManager();
		if (timeServiceManager.isPresent()) {
			timeServiceManager.get().advanceWatermark(mark);
		}
		emitter.accept(mark);
	}
}
