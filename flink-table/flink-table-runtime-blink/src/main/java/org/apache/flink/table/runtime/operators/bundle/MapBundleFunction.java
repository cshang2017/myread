

package org.apache.flink.table.runtime.operators.bundle;

import org.apache.flink.api.common.functions.Function;
import org.apache.flink.table.runtime.context.ExecutionContext;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.util.Map;

/**
 * Basic interface for map bundle processing.
 *
 * @param <K>   The type of the key in the bundle map
 * @param <V>   The type of the value in the bundle map
 * @param <IN>  Type of the input elements.
 * @param <OUT> Type of the returned elements.
 */
public abstract class MapBundleFunction<K, V, IN, OUT> implements Function {

	protected transient ExecutionContext ctx;

	public void open(ExecutionContext ctx) throws Exception {
		this.ctx = Preconditions.checkNotNull(ctx);
	}

	/**
	 * Adds the given input to the given value, returning the new bundle value.
	 *
	 * @param value the existing bundle value, maybe null
	 * @param input the given input, not null
	 */
	public abstract V addInput(@Nullable V value, IN input) throws Exception;

	/**
	 * Called when a bundle is finished. Transform a bundle to zero, one, or more output elements.
	 */
	public abstract void finishBundle(Map<K, V> buffer, Collector<OUT> out) throws Exception;

	public void close() throws Exception {}
}
