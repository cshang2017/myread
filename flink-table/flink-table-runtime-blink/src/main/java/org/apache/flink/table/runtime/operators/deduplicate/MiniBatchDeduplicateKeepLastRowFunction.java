
package org.apache.flink.table.runtime.operators.deduplicate;

import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.context.ExecutionContext;
import org.apache.flink.table.runtime.operators.bundle.MapBundleFunction;
import org.apache.flink.table.runtime.typeutils.RowDataTypeInfo;
import org.apache.flink.util.Collector;

import javax.annotation.Nullable;

import java.util.Map;

import static org.apache.flink.table.runtime.operators.deduplicate.DeduplicateFunctionHelper.processLastRow;
import static org.apache.flink.table.runtime.util.StateTtlConfigUtil.createTtlConfig;

/**
 * This function is used to get the last row for every key partition in miniBatch mode.
 */
public class MiniBatchDeduplicateKeepLastRowFunction
		extends MapBundleFunction<RowData, RowData, RowData, RowData> {

	private final RowDataTypeInfo rowTypeInfo;
	private final boolean generateUpdateBefore;
	private final boolean generateInsert;
	private final TypeSerializer<RowData> typeSerializer;
	private final long minRetentionTime;
	// state stores complete row.
	private ValueState<RowData> state;

	public MiniBatchDeduplicateKeepLastRowFunction(
			RowDataTypeInfo rowTypeInfo,
			boolean generateUpdateBefore,
			boolean generateInsert,
			TypeSerializer<RowData> typeSerializer,
			long minRetentionTime) {
		this.minRetentionTime = minRetentionTime;
		this.rowTypeInfo = rowTypeInfo;
		this.generateUpdateBefore = generateUpdateBefore;
		this.generateInsert = generateInsert;
		this.typeSerializer = typeSerializer;
	}

	@Override
	public void open(ExecutionContext ctx) throws Exception {
		super.open(ctx);
		ValueStateDescriptor<RowData> stateDesc = new ValueStateDescriptor<>("preRowState", rowTypeInfo);
		StateTtlConfig ttlConfig = createTtlConfig(minRetentionTime);
		if (ttlConfig.isEnabled()) {
			stateDesc.enableTimeToLive(ttlConfig);
		}
		state = ctx.getRuntimeContext().getState(stateDesc);
	}

	@Override
	public RowData addInput(@Nullable RowData value, RowData input) {
		// always put the input into buffer
		return typeSerializer.copy(input);
	}

	@Override
	public void finishBundle(
			Map<RowData, RowData> buffer, Collector<RowData> out) throws Exception {
		for (Map.Entry<RowData, RowData> entry : buffer.entrySet()) {
			RowData currentKey = entry.getKey();
			RowData currentRow = entry.getValue();
			ctx.setCurrentKey(currentKey);
			processLastRow(currentRow, generateUpdateBefore, generateInsert, state, out);
		}
	}
}
