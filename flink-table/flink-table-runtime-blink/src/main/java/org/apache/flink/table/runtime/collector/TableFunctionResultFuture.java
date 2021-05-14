package org.apache.flink.table.runtime.collector;

import org.apache.flink.api.common.functions.AbstractRichFunction;
import org.apache.flink.streaming.api.functions.async.ResultFuture;

/**
 * The basic implementation of collector for {@link ResultFuture} in table joining.
 */
public abstract class TableFunctionResultFuture<T> extends AbstractRichFunction implements ResultFuture<T> {

	private static final long serialVersionUID = 1L;

	private Object input;
	private ResultFuture<?> resultFuture;

	/**
	 * Sets the input row from left table,
	 * which will be used to cross join with the result of right table.
	 */
	public void setInput(Object input) {
		this.input = input;
	}

	/**
	 * Gets the input value from left table,
	 * which will be used to cross join with the result of right table.
	 */
	public Object getInput() {
		return input;
	}

	/**
	 * Sets the current collector, which used to emit the final row.
	 */
	public void setResultFuture(ResultFuture<?> resultFuture) {
		this.resultFuture = resultFuture;
	}

	/**
	 * Gets the internal collector which used to emit the final row.
	 */
	public ResultFuture<?> getResultFuture() {
		return this.resultFuture;
	}

	@Override
	public void completeExceptionally(Throwable error) {
		this.resultFuture.completeExceptionally(error);
	}
}
