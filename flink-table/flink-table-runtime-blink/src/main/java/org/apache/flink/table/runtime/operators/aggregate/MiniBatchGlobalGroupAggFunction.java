package org.apache.flink.table.runtime.operators.aggregate;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.table.data.JoinedRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.context.ExecutionContext;
import org.apache.flink.table.runtime.dataview.PerKeyStateDataViewStore;
import org.apache.flink.table.runtime.generated.AggsHandleFunction;
import org.apache.flink.table.runtime.generated.GeneratedAggsHandleFunction;
import org.apache.flink.table.runtime.generated.GeneratedRecordEqualiser;
import org.apache.flink.table.runtime.generated.RecordEqualiser;
import org.apache.flink.table.runtime.operators.bundle.MapBundleFunction;
import org.apache.flink.table.runtime.typeutils.RowDataTypeInfo;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;

import javax.annotation.Nullable;

import java.util.Map;

/*
 * Aggregate Function used for the global groupby (without window) aggregate in miniBatch mode.
 */
public class MiniBatchGlobalGroupAggFunction extends MapBundleFunction<RowData, RowData, RowData, RowData> {

	/**
	 * The code generated local function used to handle aggregates.
	 */
	private final GeneratedAggsHandleFunction genLocalAggsHandler;

	/**
	 * The code generated global function used to handle aggregates.
	 */
	private final GeneratedAggsHandleFunction genGlobalAggsHandler;

	/**
	 * The code generated equaliser used to equal RowData.
	 */
	private final GeneratedRecordEqualiser genRecordEqualiser;

	/**
	 * The accumulator types.
	 */
	private final LogicalType[] accTypes;

	/**
	 * Used to count the number of added and retracted input records.
	 */
	private final RecordCounter recordCounter;

	/**
	 * Whether this operator will generate UPDATE_BEFORE messages.
	 */
	private final boolean generateUpdateBefore;

	/**
	 * Reused output row.
	 */
	private transient JoinedRowData resultRow = new JoinedRowData();

	// local aggregate function to handle local combined accumulator rows
	private transient AggsHandleFunction localAgg = null;

	// global aggregate function to handle global accumulator rows
	private transient AggsHandleFunction globalAgg = null;

	// function used to equal RowData
	private transient RecordEqualiser equaliser = null;

	// stores the accumulators
	private transient ValueState<RowData> accState = null;

	/**
	 * Creates a {@link MiniBatchGlobalGroupAggFunction}.
	 *
	 * @param genLocalAggsHandler The generated local aggregate handler
	 * @param genGlobalAggsHandler The generated global aggregate handler
	 * @param genRecordEqualiser The code generated equaliser used to equal RowData.
	 * @param accTypes The accumulator types.
	 * @param indexOfCountStar The index of COUNT(*) in the aggregates.
	 *                          -1 when the input doesn't contain COUNT(*), i.e. doesn't contain UPDATE_BEFORE or DELETE messages.
	 *                          We make sure there is a COUNT(*) if input stream contains UPDATE_BEFORE or DELETE messages.
	 * @param generateUpdateBefore Whether this operator will generate UPDATE_BEFORE messages.
	 */
	public MiniBatchGlobalGroupAggFunction(
			GeneratedAggsHandleFunction genLocalAggsHandler,
			GeneratedAggsHandleFunction genGlobalAggsHandler,
			GeneratedRecordEqualiser genRecordEqualiser,
			LogicalType[] accTypes,
			int indexOfCountStar,
			boolean generateUpdateBefore) {
		this.genLocalAggsHandler = genLocalAggsHandler;
		this.genGlobalAggsHandler = genGlobalAggsHandler;
		this.genRecordEqualiser = genRecordEqualiser;
		this.accTypes = accTypes;
		this.recordCounter = RecordCounter.of(indexOfCountStar);
		this.generateUpdateBefore = generateUpdateBefore;
	}

	@Override
	public void open(ExecutionContext ctx) throws Exception {
		super.open(ctx);
		localAgg = genLocalAggsHandler.newInstance(ctx.getRuntimeContext().getUserCodeClassLoader());
		localAgg.open(new PerKeyStateDataViewStore(ctx.getRuntimeContext()));

		globalAgg = genGlobalAggsHandler.newInstance(ctx.getRuntimeContext().getUserCodeClassLoader());
		globalAgg.open(new PerKeyStateDataViewStore(ctx.getRuntimeContext()));

		equaliser = genRecordEqualiser.newInstance(ctx.getRuntimeContext().getUserCodeClassLoader());

		RowDataTypeInfo accTypeInfo = new RowDataTypeInfo(accTypes);
		ValueStateDescriptor<RowData> accDesc = new ValueStateDescriptor<>("accState", accTypeInfo);
		accState = ctx.getRuntimeContext().getState(accDesc);

		resultRow = new JoinedRowData();
	}

	/**
	 * The {@code previousAcc} is accumulator, but input is a row in &lt;key, accumulator&gt; schema,
	 * the specific generated {@link #localAgg} will project the {@code input} to accumulator
	 * in merge method.
	 */
	@Override
	public RowData addInput(@Nullable RowData previousAcc, RowData input) throws Exception {
		RowData currentAcc;
		if (previousAcc == null) {
			currentAcc = localAgg.createAccumulators();
		} else {
			currentAcc = previousAcc;
		}

		localAgg.setAccumulators(currentAcc);
		localAgg.merge(input);
		return localAgg.getAccumulators();
	}

	@Override
	public void finishBundle(Map<RowData, RowData> buffer, Collector<RowData> out) throws Exception {
		for (Map.Entry<RowData, RowData> entry : buffer.entrySet()) {
			RowData currentKey = entry.getKey();
			RowData bufferAcc = entry.getValue();

			boolean firstRow = false;

			// set current key to access states under the current key
			ctx.setCurrentKey(currentKey);
			RowData stateAcc = accState.value();
			if (stateAcc == null) {
				stateAcc = globalAgg.createAccumulators();
				firstRow = true;
			}
			// set accumulator first
			globalAgg.setAccumulators(stateAcc);
			// get previous aggregate result
			RowData prevAggValue = globalAgg.getValue();

			// merge bufferAcc to stateAcc
			globalAgg.merge(bufferAcc);
			// get current aggregate result
			RowData newAggValue = globalAgg.getValue();
			// get new accumulator
			stateAcc = globalAgg.getAccumulators();

			if (!recordCounter.recordCountIsZero(stateAcc)) {
				// we aggregated at least one record for this key

				// update acc to state
				accState.update(stateAcc);

				// if this was not the first row and we have to emit retractions
				if (!firstRow) {
					if (!equaliser.equals(prevAggValue, newAggValue)) {
						// new row is not same with prev row
						if (generateUpdateBefore) {
							// prepare UPDATE_BEFORE message for previous row
							resultRow.replace(currentKey, prevAggValue).setRowKind(RowKind.UPDATE_BEFORE);
							out.collect(resultRow);
						}
						// prepare UPDATE_AFTER message for new row
						resultRow.replace(currentKey, newAggValue).setRowKind(RowKind.UPDATE_AFTER);
						out.collect(resultRow);
					}
					// new row is same with prev row, no need to output
				} else {
					// this is the first, output new result
					// prepare INSERT message for new row
					resultRow.replace(currentKey, newAggValue).setRowKind(RowKind.INSERT);
					out.collect(resultRow);
				}

			} else {
				// we retracted the last record for this key
				// sent out a delete message
				if (!firstRow) {
					// prepare DELETE message for previous row
					resultRow.replace(currentKey, prevAggValue).setRowKind(RowKind.DELETE);
					out.collect(resultRow);
				}
				// and clear all state
				accState.clear();
				// cleanup dataview under current key
				globalAgg.cleanup();
			}
		}
	}

	@Override
	public void close() throws Exception {
		if (localAgg != null) {
			localAgg.close();
		}
		if (globalAgg != null) {
			globalAgg.close();
		}
	}
}
