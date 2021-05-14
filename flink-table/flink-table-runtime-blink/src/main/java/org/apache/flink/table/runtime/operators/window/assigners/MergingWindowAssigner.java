package org.apache.flink.table.runtime.operators.window.assigners;

import org.apache.flink.table.runtime.operators.window.Window;

import java.util.Collection;
import java.util.NavigableSet;

/**
 * A {@code WindowAssigner} that can merge windows.
 *
 * @param <W> The type of {@code Window} that this assigner assigns.
 */
public abstract class MergingWindowAssigner<W extends Window> extends WindowAssigner<W> {

	/**
	 * Determines which windows (if any) should be merged.
	 *
	 * @param newWindow The new window
	 * @param sortedWindows The sorted window candidates.
	 * @param callback A callback that can be invoked to signal which windows should be merged.
	 */
	public abstract void mergeWindows(W newWindow, NavigableSet<W> sortedWindows, MergeCallback<W> callback);

	// ------------------------------------------------------------------------

	/**
	 * Callback to be used in {@link #mergeWindows(Window, NavigableSet, MergeCallback)} for specifying which
	 * windows should be merged.
	 */
	public interface MergeCallback<W> {

		/**
		 * Specifies that the given windows should be merged into the result window.
		 *
		 * @param toBeMerged The list of windows that should be merged into one window.
		 * @param mergeResult The resulting merged window.
		 */
		void merge(W mergeResult, Collection<W> toBeMerged);
	}
}
