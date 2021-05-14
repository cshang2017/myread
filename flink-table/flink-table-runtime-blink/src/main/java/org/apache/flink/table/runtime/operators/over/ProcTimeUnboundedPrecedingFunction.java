package org.apache.flink.table.runtime.operators.over;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.table.data.JoinedRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.dataview.PerKeyStateDataViewStore;
import org.apache.flink.table.runtime.functions.KeyedProcessFunctionWithCleanupState;
import org.apache.flink.table.runtime.generated.AggsHandleFunction;
import org.apache.flink.table.runtime.generated.GeneratedAggsHandleFunction;
import org.apache.flink.table.runtime.typeutils.RowDataTypeInfo;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.util.Collector;

/**
 * Process Function for processing-time unbounded OVER window.
 *
 * <p>E.g.:
 * SELECT currtime, b, c,
 * min(c) OVER
 * (PARTITION BY b ORDER BY proctime ROWS BETWEEN UNBOUNDED preceding AND CURRENT ROW),
 * max(c) OVER
 * (PARTITION BY b ORDER BY proctime ROWS BETWEEN UNBOUNDED preceding AND CURRENT ROW)
 * FROM T.
 */
public class ProcTimeUnboundedPrecedingFunction<K> extends KeyedProcessFunctionWithCleanupState<K, RowData, RowData> {

	private final GeneratedAggsHandleFunction genAggsHandler;
	private final LogicalType[] accTypes;

	private transient AggsHandleFunction function;
	private transient ValueState<RowData> accState;
	private transient JoinedRowData output;

	public ProcTimeUnboundedPrecedingFunction(
			long minRetentionTime,
			long maxRetentionTime,
			GeneratedAggsHandleFunction genAggsHandler,
			LogicalType[] accTypes) {
		super(minRetentionTime, maxRetentionTime);
		this.genAggsHandler = genAggsHandler;
		this.accTypes = accTypes;
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		function = genAggsHandler.newInstance(getRuntimeContext().getUserCodeClassLoader());
		function.open(new PerKeyStateDataViewStore(getRuntimeContext()));

		output = new JoinedRowData();

		RowDataTypeInfo accTypeInfo = new RowDataTypeInfo(accTypes);
		ValueStateDescriptor<RowData> stateDescriptor =
			new ValueStateDescriptor<RowData>("accState", accTypeInfo);
		accState = getRuntimeContext().getState(stateDescriptor);

		initCleanupTimeState("ProcTimeUnboundedOverCleanupTime");
	}

	@Override
	public void processElement(
			RowData input,
			KeyedProcessFunction<K, RowData, RowData>.Context ctx,
			Collector<RowData> out) throws Exception {
		// register state-cleanup timer
		registerProcessingCleanupTimer(ctx, ctx.timerService().currentProcessingTime());

		RowData accumulators = accState.value();
		if (null == accumulators) {
			accumulators = function.createAccumulators();
		}
		// set accumulators in context first
		function.setAccumulators(accumulators);

		// accumulate input row
		function.accumulate(input);

		// update the value of accumulators for future incremental computation
		accumulators = function.getAccumulators();
		accState.update(accumulators);

		// prepare output row
		RowData aggValue = function.getValue();
		output.replace(input, aggValue);
		out.collect(output);
	}

	@Override
	public void onTimer(
			long timestamp,
			KeyedProcessFunction<K, RowData, RowData>.OnTimerContext ctx,
			Collector<RowData> out) throws Exception {
		if (stateCleaningEnabled) {
			cleanupState(accState);
			function.cleanup();
		}
	}

	@Override
	public void close() throws Exception {
		if (null != function) {
			function.close();
		}
	}
}
