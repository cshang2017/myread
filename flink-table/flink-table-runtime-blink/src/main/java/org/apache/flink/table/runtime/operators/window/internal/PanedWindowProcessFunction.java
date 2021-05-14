package org.apache.flink.table.runtime.operators.window.internal;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.generated.NamespaceAggsHandleFunctionBase;
import org.apache.flink.table.runtime.operators.window.Window;
import org.apache.flink.table.runtime.operators.window.assigners.PanedWindowAssigner;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * The implementation of {@link InternalWindowProcessFunction} for {@link PanedWindowAssigner}.
 * @param <W> The type of {@code Window} that assigner assigns.
 */
public class PanedWindowProcessFunction<K, W extends Window>
	extends InternalWindowProcessFunction<K, W> {

	private final PanedWindowAssigner<W> windowAssigner;

	public PanedWindowProcessFunction(
			PanedWindowAssigner<W> windowAssigner,
			NamespaceAggsHandleFunctionBase<W> windowAggregator,
			long allowedLateness) {
		super(windowAssigner, windowAggregator, allowedLateness);
		this.windowAssigner = windowAssigner;
	}

	@Override
	public Collection<W> assignActualWindows(RowData inputRow, long timestamp) throws Exception {
		Collection<W> elementWindows = windowAssigner.assignWindows(inputRow, timestamp);
		List<W> actualWindows = new ArrayList<>(elementWindows.size());
		for (W window : elementWindows) {
			if (!isWindowLate(window)) {
				actualWindows.add(window);
			}
		}
		return actualWindows;
	}

	@Override
	public Collection<W> assignStateNamespace(RowData inputRow, long timestamp) throws Exception {
		W pane = windowAssigner.assignPane(inputRow, timestamp);
		if (!isPaneLate(pane)) {
			return Collections.singleton(pane);
		} else {
			return Collections.emptyList();
		}
	}

	@Override
	public void prepareAggregateAccumulatorForEmit(W window) throws Exception {
		Iterable<W> panes = windowAssigner.splitIntoPanes(window);
		RowData acc = windowAggregator.createAccumulators();
		// null namespace means use heap data views
		windowAggregator.setAccumulators(null, acc);
		for (W pane : panes) {
			RowData paneAcc = ctx.getWindowAccumulators(pane);
			if (paneAcc != null) {
				windowAggregator.merge(pane, paneAcc);
			}
		}
	}

	@Override
	public void cleanWindowIfNeeded(W window, long currentTime) throws Exception {
		if (isCleanupTime(window, currentTime)) {
			Iterable<W> panes = windowAssigner.splitIntoPanes(window);
			for (W pane : panes) {
				W lastWindow = windowAssigner.getLastWindow(pane);
				if (window.equals(lastWindow)) {
					ctx.clearWindowState(pane);
				}
			}
			ctx.clearTrigger(window);
			ctx.clearPreviousState(window);
		}
	}

	/** checks whether the pane is late (e.g. can be / has been cleanup) */
	private boolean isPaneLate(W pane) {
		// whether the pane is late depends on the last window which the pane is belongs to is late
		return windowAssigner.isEventTime() && isWindowLate(windowAssigner.getLastWindow(pane));
	}
}
