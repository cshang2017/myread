

package org.apache.flink.runtime.resourcemanager;

import org.apache.flink.configuration.Configuration;

/**
 * Implementation of {@link WorkerResourceSpecFactory} that creates arbitrary {@link WorkerResourceSpec}.
 * Used for scenarios where the values in the default {@link WorkerResourceSpec} does not matter.
 */
public class ArbitraryWorkerResourceSpecFactory extends WorkerResourceSpecFactory {

	public static final ArbitraryWorkerResourceSpecFactory INSTANCE = new ArbitraryWorkerResourceSpecFactory();

	private ArbitraryWorkerResourceSpecFactory() {}

	@Override
	public WorkerResourceSpec createDefaultWorkerResourceSpec(Configuration configuration) {
		return WorkerResourceSpec.ZERO;
	}
}
