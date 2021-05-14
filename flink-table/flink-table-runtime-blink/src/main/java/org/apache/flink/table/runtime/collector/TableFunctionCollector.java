package org.apache.flink.table.runtime.collector;

import org.apache.flink.api.common.functions.AbstractRichFunction;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.util.Collector;

/**
 * The basic implementation of collector for {@link TableFunction}.
 */
public abstract class TableFunctionCollector<T> extends AbstractRichFunction implements Collector<T> {

	private Object input;
	private Collector collector;
	private boolean collected;

	/**
	 * Sets the input row from left table,
	 * which will be used to cross join with the result of table function.
	 */
	public void setInput(Object input) {
		this.input = input;
	}

	/**
	 * Gets the input value from left table,
	 * which will be used to cross join with the result of table function.
	 */
	public Object getInput() {
		return input;
	}

	/**
	 * Sets the current collector, which used to emit the final row.
	 */
	public void setCollector(Collector<?> collector) {
		this.collector = collector;
	}

	/**
	 * Resets the flag to indicate whether [[collect(T)]] has been called.
	 */
	public void reset() {
		this.collected = false;
		if (collector instanceof TableFunctionCollector) {
			((TableFunctionCollector) collector).reset();
		}
	}

	/**
	 * Output final result of this UDTF to downstreams.
	 */
	@SuppressWarnings("unchecked")
	public void outputResult(Object result) {
		this.collected = true;
		this.collector.collect(result);
	}

	/**
	 * Whether {@link #collect(Object)} has been called.
	 *
	 * @return True if {@link #collect(Object)} has been called.
	 */
	public boolean isCollected() {
		return collected;
	}

	public void close() {
		this.collector.close();
	}
}
