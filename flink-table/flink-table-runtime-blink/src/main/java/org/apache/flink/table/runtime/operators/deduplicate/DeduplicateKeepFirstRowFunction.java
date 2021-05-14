
package org.apache.flink.table.runtime.operators.deduplicate;

import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Collector;

import static org.apache.flink.table.runtime.operators.deduplicate.DeduplicateFunctionHelper.processFirstRow;
import static org.apache.flink.table.runtime.util.StateTtlConfigUtil.createTtlConfig;

/**
 * This function is used to deduplicate on keys and keeps only first row.
 */
public class DeduplicateKeepFirstRowFunction
		extends KeyedProcessFunction<RowData, RowData, RowData> {

	private final long minRetentionTime;
	// state stores a boolean flag to indicate whether key appears before.
	private ValueState<Boolean> state;

	public DeduplicateKeepFirstRowFunction(long minRetentionTime) {
		this.minRetentionTime = minRetentionTime;
	}

	@Override
	public void open(Configuration configure) throws Exception {
		super.open(configure);
		ValueStateDescriptor<Boolean> stateDesc = new ValueStateDescriptor<>("existsState", Types.BOOLEAN);
		StateTtlConfig ttlConfig = createTtlConfig(minRetentionTime);
		if (ttlConfig.isEnabled()) {
			stateDesc.enableTimeToLive(ttlConfig);
		}
		state = getRuntimeContext().getState(stateDesc);
	}

	@Override
	public void processElement(RowData input, Context ctx, Collector<RowData> out) throws Exception {
		processFirstRow(input, state, out);
	}
}
