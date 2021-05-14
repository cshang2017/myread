
package org.apache.flink.table.runtime.operators.bundle;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.table.runtime.operators.bundle.trigger.BundleTrigger;

/**
 * The {@link MapBundleOperator} uses a {@link KeySelector} to extract bundle key, thus can be
 * used with non-keyed-stream.
 */
public class MapBundleOperator<K, V, IN, OUT> extends AbstractMapBundleOperator<K, V, IN, OUT> {

	/** KeySelector is used to extract key for bundle map. */
	private final KeySelector<IN, K> keySelector;

	public MapBundleOperator(
			MapBundleFunction<K, V, IN, OUT> function,
			BundleTrigger<IN> bundleTrigger,
			KeySelector<IN, K> keySelector) {
		super(function, bundleTrigger);
		this.keySelector = keySelector;
	}

	@Override
	protected K getKey(IN input) throws Exception {
		return this.keySelector.getKey(input);
	}
}
