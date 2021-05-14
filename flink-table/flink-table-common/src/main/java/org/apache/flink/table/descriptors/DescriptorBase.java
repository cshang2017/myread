package org.apache.flink.table.descriptors;

import org.apache.flink.annotation.PublicEvolving;

/**
 * Base class for {@link Descriptor}s.
 */
@PublicEvolving
public abstract class DescriptorBase implements Descriptor {

	@Override
	public String toString() {
		return DescriptorProperties.toString(toProperties());
	}
}
