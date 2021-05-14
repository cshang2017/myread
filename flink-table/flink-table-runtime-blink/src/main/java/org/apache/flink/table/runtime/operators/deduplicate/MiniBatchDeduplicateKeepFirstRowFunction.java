
package org.apache.flink.table.runtime.operators.deduplicate;

import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.context.ExecutionContext;
import org.apache.flink.table.runtime.operators.bundle.MapBundleFunction;
import org.apache.flink.util.Collector;

import javax.annotation.Nullable;

import java.util.Map;

import static org.apache.flink.table.runtime.operators.deduplicate.DeduplicateFunctionHelper.processFirstRow;
import static org.apache.flink.table.runtime.util.StateTtlConfigUtil.createTtlConfig;

/**
 * This function is used to get the first row for every key partition in miniBatch mode.
 */
public class MiniBatchDeduplicateKeepFirstRowFunction
		extends MapBundleFunction<RowData, RowData, RowData, RowData> {

	private final TypeSerializer<RowData> typeSerializer;
	private final long minRetentionTime;
	// state stores a boolean flag to indicate whether key appears before.
	private ValueState<Boolean> state;

	public MiniBatchDeduplicateKeepFirstRowFunction(
			TypeSerializer<RowData> typeSerializer,
			long minRetentionTime) {
		this.minRetentionTime = minRetentionTime;
		this.typeSerializer = typeSerializer;
	}

	@Override
	public void open(ExecutionContext ctx) throws Exception {
		super.open(ctx);
		ValueStateDescriptor<Boolean> stateDesc = new ValueStateDescriptor<>("existsState", Types.BOOLEAN);
		StateTtlConfig ttlConfig = createTtlConfig(minRetentionTime);
		if (ttlConfig.isEnabled()) {
			stateDesc.enableTimeToLive(ttlConfig);
		}
		state = ctx.getRuntimeContext().getState(stateDesc);
	}

	@Override
	public RowData addInput(@Nullable RowData value, RowData input) {
		if (value == null) {
			// put the input into buffer
			return typeSerializer.copy(input);
		} else {
			// the input is not first row, ignore it
			return value;
		}
	}

	@Override
	public void finishBundle(
			Map<RowData, RowData> buffer, Collector<RowData> out) throws Exception {
		for (Map.Entry<RowData, RowData> entry : buffer.entrySet()) {
			RowData currentKey = entry.getKey();
			RowData currentRow = entry.getValue();
			ctx.setCurrentKey(currentKey);
			processFirstRow(currentRow, state, out);
		}
	}
}
