package org.apache.flink.streaming.api.operators.source;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.streaming.runtime.io.PushingAsyncDataInput;
import org.apache.flink.streaming.runtime.streamstatus.StreamStatus;
import org.apache.flink.streaming.runtime.tasks.ExceptionInChainedOperatorException;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * An adapter that exposes a {@link WatermarkOutput} based on a {@link PushingAsyncDataInput.DataOutput}.
 */
@Internal
public final class WatermarkToDataOutput implements WatermarkOutput {

	private final PushingAsyncDataInput.DataOutput<?> output;
	private long maxWatermarkSoFar;
	private boolean isIdle;

	/**
	 * Creates a new WatermarkOutput against the given DataOutput.
	 */
	public WatermarkToDataOutput(PushingAsyncDataInput.DataOutput<?> output) {
		this.output = checkNotNull(output);
		this.maxWatermarkSoFar = Long.MIN_VALUE;
	}

	@Override
	public void emitWatermark(Watermark watermark) {
		final long newWatermark = watermark.getTimestamp();
		if (newWatermark <= maxWatermarkSoFar) {
			return;
		}

		maxWatermarkSoFar = newWatermark;

		try {
			if (isIdle) {
				output.emitStreamStatus(StreamStatus.ACTIVE);
				isIdle = false;
			}

			output.emitWatermark(new org.apache.flink.streaming.api.watermark.Watermark(newWatermark));
		} catch (ExceptionInChainedOperatorException e) {
			throw e;
		} catch (Exception e) {
			throw new ExceptionInChainedOperatorException(e);
		}
	}

	@Override
	public void markIdle() {
		if (isIdle) {
			return;
		}

		try {
			output.emitStreamStatus(StreamStatus.IDLE);
			isIdle = true;
		} catch (ExceptionInChainedOperatorException e) {
			throw e;
		} catch (Exception e) {
			throw new ExceptionInChainedOperatorException(e);
		}
	}
}
