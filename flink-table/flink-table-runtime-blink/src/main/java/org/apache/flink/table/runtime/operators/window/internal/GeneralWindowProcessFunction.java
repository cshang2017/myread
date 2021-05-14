package org.apache.flink.table.runtime.operators.window.internal;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.generated.NamespaceAggsHandleFunctionBase;
import org.apache.flink.table.runtime.operators.window.Window;
import org.apache.flink.table.runtime.operators.window.assigners.WindowAssigner;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * The general implementation of {@link InternalWindowProcessFunction}. The {@link WindowAssigner}
 * should be a regular assigner without implement {@code PanedWindowAssigner} or {@code MergingWindowAssigner}.
 * @param <W> The type of {@code Window} that assigner assigns.
 */
public class GeneralWindowProcessFunction<K, W extends Window>
	extends InternalWindowProcessFunction<K, W> {

	private List<W> reuseAffectedWindows;

	public GeneralWindowProcessFunction(
			WindowAssigner<W> windowAssigner,
			NamespaceAggsHandleFunctionBase<W> windowAggregator,
			long allowedLateness) {
		super(windowAssigner, windowAggregator, allowedLateness);
	}

	@Override
	public Collection<W> assignStateNamespace(RowData inputRow, long timestamp) throws Exception {
		Collection<W> elementWindows = windowAssigner.assignWindows(inputRow, timestamp);
		reuseAffectedWindows = new ArrayList<>(elementWindows.size());
		for (W window : elementWindows) {
			if (!isWindowLate(window)) {
				reuseAffectedWindows.add(window);
			}
		}
		return reuseAffectedWindows;
	}

	@Override
	public Collection<W> assignActualWindows(RowData inputRow, long timestamp) throws Exception {
		// actual windows is equal to affected window, reuse it
		return reuseAffectedWindows;
	}

	@Override
	public void prepareAggregateAccumulatorForEmit(W window) throws Exception {
		RowData acc = ctx.getWindowAccumulators(window);
		if (acc == null) {
			acc = windowAggregator.createAccumulators();
		}
		windowAggregator.setAccumulators(window, acc);
	}

	@Override
	public void cleanWindowIfNeeded(W window, long time) throws Exception {
		if (isCleanupTime(window, time)) {
			ctx.clearWindowState(window);
			ctx.clearPreviousState(window);
			ctx.clearTrigger(window);
		}
	}
}
