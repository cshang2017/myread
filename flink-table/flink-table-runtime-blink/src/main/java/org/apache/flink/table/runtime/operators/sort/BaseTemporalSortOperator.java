package org.apache.flink.table.runtime.operators.sort;

import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.streaming.api.SimpleTimerService;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.InternalTimerService;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.TimestampedCollector;
import org.apache.flink.streaming.api.operators.Triggerable;
import org.apache.flink.table.data.RowData;

/**
 * Base class for stream temporal sort operator.
 */
abstract class BaseTemporalSortOperator extends AbstractStreamOperator<RowData> implements
		OneInputStreamOperator<RowData, RowData>, Triggerable<RowData, VoidNamespace> {

	protected transient TimerService timerService;
	protected transient TimestampedCollector<RowData> collector;

	@Override
	public void open() throws Exception {
		InternalTimerService<VoidNamespace> internalTimerService = getInternalTimerService("user-timers",
				VoidNamespaceSerializer.INSTANCE,
				this);
		timerService = new SimpleTimerService(internalTimerService);
		collector = new TimestampedCollector<>(output);
	}

}
