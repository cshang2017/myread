package org.apache.flink.table.runtime.operators.wmassigners;

import org.apache.flink.metrics.Gauge;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeCallback;
import org.apache.flink.table.data.RowData;

/**
 * A stream operator that emits mini-batch marker in a given period.
 * This mini-batch assigner works in processing time, which means the mini-batch marker is generated
 * in the given period using the processing time. The downstream operators will trigger mini-batch
 * once the received mini-batch id advanced.
 *
 * <p>NOTE: currently, we use {@link Watermark} to represents the mini-batch marker.
 *
 * <p>The difference between this operator and {@link RowTimeMiniBatchAssginerOperator} is that,
 * this operator generates watermarks by itself using processing time, but the other forwards
 * watermarks from upstream.
 */
public class ProcTimeMiniBatchAssignerOperator extends AbstractStreamOperator<RowData>
	implements OneInputStreamOperator<RowData, RowData>, ProcessingTimeCallback {

	private final long intervalMs;
	private transient long currentWatermark;

	public ProcTimeMiniBatchAssignerOperator(long intervalMs) {
		this.intervalMs = intervalMs;
		this.chainingStrategy = ChainingStrategy.ALWAYS;
	}

	@Override
	public void open() throws Exception {
		super.open();

		currentWatermark = 0;

		long now = getProcessingTimeService().getCurrentProcessingTime();
		getProcessingTimeService().registerTimer(now + intervalMs, this);

	}

	@Override
	public void processElement(StreamRecord<RowData> element) throws Exception {
		long now = getProcessingTimeService().getCurrentProcessingTime();
		long currentBatch = now - now % intervalMs;
		if (currentBatch > currentWatermark) {
			currentWatermark = currentBatch;
			// emit
			output.emitWatermark(new Watermark(currentBatch));
		}
		output.collect(element);
	}

	@Override
	public void onProcessingTime(long timestamp) throws Exception {
		long now = getProcessingTimeService().getCurrentProcessingTime();
		long currentBatch = now - now % intervalMs;
		if (currentBatch > currentWatermark) {
			currentWatermark = currentBatch;
			// emit
			output.emitWatermark(new Watermark(currentBatch));
		}
		getProcessingTimeService().registerTimer(currentBatch + intervalMs, this);
	}

	/**
	 * Override the base implementation to completely ignore watermarks propagated from
	 * upstream (we rely only on the {@link AssignerWithPeriodicWatermarks} to emit
	 * watermarks from here).
	 */
	@Override
	public void processWatermark(Watermark mark) throws Exception {
		// if we receive a Long.MAX_VALUE watermark we forward it since it is used
		// to signal the end of input and to not block watermark progress downstream
		if (mark.getTimestamp() == Long.MAX_VALUE && currentWatermark != Long.MAX_VALUE) {
			currentWatermark = Long.MAX_VALUE;
			output.emitWatermark(mark);
		}
	}

	@Override
	public void close() throws Exception {
		super.close();
	}
}
