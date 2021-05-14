

package org.apache.flink.table.runtime.operators.bundle;

import org.apache.flink.table.runtime.operators.bundle.trigger.BundleTrigger;

/**
 * The {@link KeyedMapBundleOperator} uses framework's key as bundle map key, thus can only be
 * used on {@link org.apache.flink.streaming.api.datastream.KeyedStream}.
 */
public class KeyedMapBundleOperator<K, V, IN, OUT> extends AbstractMapBundleOperator<K, V, IN, OUT> {

	public KeyedMapBundleOperator(
			MapBundleFunction<K, V, IN, OUT> function,
			BundleTrigger<IN> bundleTrigger) {
		super(function, bundleTrigger);
	}

	@Override
	protected K getKey(IN input) throws Exception {
		return (K) getCurrentKey();
	}
}
