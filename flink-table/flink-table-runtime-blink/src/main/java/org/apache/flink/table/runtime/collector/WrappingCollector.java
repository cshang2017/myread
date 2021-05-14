

package org.apache.flink.table.runtime.collector;

import org.apache.flink.api.common.functions.AbstractRichFunction;
import org.apache.flink.util.Collector;

/**
 * A {@link Collector} that wraps another collector. An implementation can decide when to emit to the
 * wrapped collector.
 */
public abstract class WrappingCollector<T> extends AbstractRichFunction implements Collector<T> {

	private Collector<T> collector;

	/**
	 * Sets the current collector which is used to emit the final result.
	 */
	public void setCollector(Collector<T> collector) {
		this.collector = collector;
	}

	/**
	 * Outputs the final result to the wrapped collector.
	 */
	public void outputResult(T result) {
		this.collector.collect(result);
	}

	@Override
	public void close() {
		this.collector.close();
	}
}
