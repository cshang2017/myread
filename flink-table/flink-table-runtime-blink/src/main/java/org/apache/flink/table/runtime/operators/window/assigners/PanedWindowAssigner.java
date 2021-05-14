package org.apache.flink.table.runtime.operators.window.assigners;

import org.apache.flink.table.runtime.operators.window.Window;

/**
 * A {@code WindowAssigner} that window can be split into panes.
 *
 * @param <W> The type of {@code Window} that this assigner assigns.
 */
public abstract class PanedWindowAssigner<W extends Window> extends WindowAssigner<W> {

	/**
	 * Given the timestamp and element, returns the pane into which it should be placed.
	 * @param element The element to which windows should be assigned.
	 * @param timestamp The timestamp of the element when {@link #isEventTime()} returns true,
	 *                  or the current system time when {@link #isEventTime()} returns false.
	 */
	public abstract W assignPane(Object element, long timestamp);

	/**
	 * Splits the given window into panes collection.
	 * @param window the window to be split.
	 * @return the panes iterable
	 */
	public abstract Iterable<W> splitIntoPanes(W window);

	/**
	 * Gets the last window which the pane belongs to.
	 */
	public abstract W getLastWindow(W pane);
}
