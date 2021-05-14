package org.apache.flink.streaming.api.operators.async.queue;

import org.apache.flink.annotation.Internal;
import org.apache.flink.streaming.api.operators.TimestampedCollector;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nonnull;

import java.util.Collection;

/**
 * {@link StreamElementQueueEntry} implementation for the {@link Watermark}.
 */
@Internal
class WatermarkQueueEntry<OUT> implements StreamElementQueueEntry<OUT> {
	@Nonnull
	private final Watermark watermark;

	WatermarkQueueEntry(Watermark watermark) {
		this.watermark = Preconditions.checkNotNull(watermark);
	}

	@Override
	public void emitResult(TimestampedCollector<OUT> output) {
		output.emitWatermark(watermark);
	}

	@Nonnull
	@Override
	public Watermark getInputElement() {
		return watermark;
	}

	@Override
	public boolean isDone() {
		return true;
	}

	@Override
	public void complete(Collection result) {
		throw new IllegalStateException("Cannot complete a watermark.");
	}
}
