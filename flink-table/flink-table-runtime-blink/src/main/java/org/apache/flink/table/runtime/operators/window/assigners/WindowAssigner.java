package org.apache.flink.table.runtime.operators.window.assigners;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.operators.window.Window;
import org.apache.flink.table.runtime.operators.window.internal.InternalWindowProcessFunction;
import org.apache.flink.table.runtime.operators.window.triggers.Trigger;

import java.io.IOException;
import java.io.Serializable;
import java.util.Collection;

/**
 * A {@code WindowAssigner} assigns zero or more {@link Window Windows} to an element.
 *
 * <p>In a window operation, elements are grouped by their key (if available) and by the windows to
 * which it was assigned. The set of elements with the same key and window is called a pane.
 * When a {@link Trigger} decides that a certain pane should fire the window to produce
 * output elements for that pane.
 *
 * @param <W> The type of {@code Window} that this assigner assigns.
 */
public abstract class WindowAssigner<W extends Window> implements Serializable {

	/**
	 * Initialization method for the function. It is called before the actual working methods.
	 */
	public void open(InternalWindowProcessFunction.Context<?, W> ctx) throws Exception {
		// nothing to do
	}

	/**
	 * Given the timestamp and element, returns the set of windows into which it
	 * should be placed.
	 *
	 * @param element The element to which windows should be assigned.
	 * @param timestamp The timestamp of the element when {@link #isEventTime()} returns true,
	 *                  or the current system time when {@link #isEventTime()} returns false.
	 */
	public abstract Collection<W> assignWindows(RowData element, long timestamp) throws IOException;

	/**
	 * Returns a {@link TypeSerializer} for serializing windows that are assigned by
	 * this {@code WindowAssigner}.
	 */
	public abstract TypeSerializer<W> getWindowSerializer(ExecutionConfig executionConfig);

	/**
	 * Returns {@code true} if elements are assigned to windows based on event time,
	 * {@code false} otherwise.
	 */
	public abstract boolean isEventTime();

	public abstract String toString();
}
