package org.apache.flink.table.runtime.operators.aggregate;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.table.data.JoinedRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.context.ExecutionContext;
import org.apache.flink.table.runtime.dataview.PerKeyStateDataViewStore;
import org.apache.flink.table.runtime.generated.AggsHandleFunction;
import org.apache.flink.table.runtime.generated.GeneratedAggsHandleFunction;
import org.apache.flink.table.runtime.operators.bundle.MapBundleFunction;
import org.apache.flink.util.Collector;

import javax.annotation.Nullable;

import java.util.HashMap;
import java.util.Map;

/**
 * Aggregate Function used for the incremental groupby (without window) aggregate in miniBatch mode.
 */
public class MiniBatchIncrementalGroupAggFunction extends MapBundleFunction<RowData, RowData, RowData, RowData> {

	/**
	 * The code generated partial function used to handle aggregates.
	 */
	private final GeneratedAggsHandleFunction genPartialAggsHandler;

	/**
	 * The code generated final function used to handle aggregates.
	 */
	private final GeneratedAggsHandleFunction genFinalAggsHandler;

	/**
	 * The key selector to extract final key.
	 */
	private final KeySelector<RowData, RowData> finalKeySelector;

	/**
	 * Reused output row.
	 */
	private transient JoinedRowData resultRow = new JoinedRowData();

	// local aggregate function to handle local combined accumulator rows
	private transient AggsHandleFunction partialAgg = null;

	// global aggregate function to handle global accumulator rows
	private transient AggsHandleFunction finalAgg = null;

	public MiniBatchIncrementalGroupAggFunction(
			GeneratedAggsHandleFunction genPartialAggsHandler,
			GeneratedAggsHandleFunction genFinalAggsHandler,
			KeySelector<RowData, RowData> finalKeySelector) {
		this.genPartialAggsHandler = genPartialAggsHandler;
		this.genFinalAggsHandler = genFinalAggsHandler;
		this.finalKeySelector = finalKeySelector;
	}

	@Override
	public void open(ExecutionContext ctx) throws Exception {
		super.open(ctx);
		ClassLoader classLoader = ctx.getRuntimeContext().getUserCodeClassLoader();
		partialAgg = genPartialAggsHandler.newInstance(classLoader);
		partialAgg.open(new PerKeyStateDataViewStore(ctx.getRuntimeContext()));

		finalAgg = genFinalAggsHandler.newInstance(classLoader);
		finalAgg.open(new PerKeyStateDataViewStore(ctx.getRuntimeContext()));

		resultRow = new JoinedRowData();
	}

	@Override
	public RowData addInput(@Nullable RowData previousAcc, RowData input) throws Exception {
		RowData currentAcc;
		if (previousAcc == null) {
			currentAcc = partialAgg.createAccumulators();
		} else {
			currentAcc = previousAcc;
		}

		partialAgg.setAccumulators(currentAcc);
		partialAgg.merge(input);
		return partialAgg.getAccumulators();
	}

	@Override
	public void finishBundle(Map<RowData, RowData> buffer, Collector<RowData> out) throws Exception {
		// pre-aggregate for final aggregate result

		// buffer schema: [finalKey, [partialKey, partialAcc]]
		Map<RowData, Map<RowData, RowData>> finalAggBuffer = new HashMap<>();
		for (Map.Entry<RowData, RowData> entry : buffer.entrySet()) {
			RowData partialKey = entry.getKey();
			RowData finalKey = finalKeySelector.getKey(partialKey);
			RowData partialAcc = entry.getValue();
			// use compute to avoid additional put
			Map<RowData, RowData> accMap = finalAggBuffer.computeIfAbsent(finalKey, r -> new HashMap<>());
			accMap.put(partialKey, partialAcc);
		}

		for (Map.Entry<RowData, Map<RowData, RowData>> entry : finalAggBuffer.entrySet()) {
			RowData finalKey = entry.getKey();
			Map<RowData, RowData> accMap = entry.getValue();
			// set accumulators to initial value
			finalAgg.resetAccumulators();
			for (Map.Entry<RowData, RowData> accEntry : accMap.entrySet()) {
				RowData partialKey = accEntry.getKey();
				RowData partialAcc = accEntry.getValue();
				// set current key to make dataview know current key
				ctx.setCurrentKey(partialKey);
				finalAgg.merge(partialAcc);
			}
			RowData finalAcc = finalAgg.getAccumulators();
			resultRow.replace(finalKey, finalAcc);
			out.collect(resultRow);
		}
		// for gc friendly
		finalAggBuffer.clear();
	}

	@Override
	public void close() throws Exception {
		if (partialAgg != null) {
			partialAgg.close();
		}
		if (finalAgg != null) {
			finalAgg.close();
		}
	}
}
