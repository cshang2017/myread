package org.apache.flink.table.runtime.operators.sort;

import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.operators.TableStreamOperator;
import org.apache.flink.table.runtime.util.StreamRecordCollector;
import org.apache.flink.util.Collector;

/**
 * Operator for batch limit.
 * TODO support stopEarly.
 */
public class LimitOperator extends TableStreamOperator<RowData>
		implements OneInputStreamOperator<RowData, RowData> {

	private final boolean isGlobal;
	private final long limitStart;
	private final long limitEnd;

	private transient Collector<RowData> collector;
	private transient int count = 0;

	public LimitOperator(boolean isGlobal, long limitStart, long limitEnd) {
		this.isGlobal = isGlobal;
		this.limitStart = limitStart;
		this.limitEnd = limitEnd;
	}

	@Override
	public void open() throws Exception {
		super.open();
		this.collector = new StreamRecordCollector<>(output);
	}

	@Override
	public void processElement(StreamRecord<RowData> element) throws Exception {
		if (count < limitEnd) {
			count++;
			if (!isGlobal || count > limitStart) {
				collector.collect(element.getValue());
			}
		}
	}
}
