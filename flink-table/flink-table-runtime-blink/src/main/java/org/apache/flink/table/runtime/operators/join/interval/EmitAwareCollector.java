package org.apache.flink.table.runtime.operators.join.interval;

import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Collector;

/**
 * Collector to wrap a [[org.apache.flink.table.dataformat.RowData]] and to track whether a row has been
 * emitted by the inner collector.
 */
class EmitAwareCollector implements Collector<RowData> {

	private boolean emitted = false;
	private Collector<RowData> innerCollector;

	void reset() {
		emitted = false;
	}

	boolean isEmitted() {
		return emitted;
	}

	void setInnerCollector(Collector<RowData> innerCollector) {
		this.innerCollector = innerCollector;
	}

	@Override
	public void collect(RowData record) {
		emitted = true;
		innerCollector.collect(record);
	}

	@Override
	public void close() {
		innerCollector.close();
	}

}
