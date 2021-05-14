
package org.apache.flink.api.common.resources;

import org.apache.flink.annotation.Internal;

import java.math.BigDecimal;

/**
 * The GPU resource.
 */
@Internal
public class GPUResource extends Resource {

	public static final String NAME = "GPU";

	public GPUResource(double value) {
		super(NAME, value);
	}

	private GPUResource(BigDecimal value) {
		super(NAME, value);
	}

	@Override
	public Resource create(BigDecimal value) {
		return new GPUResource(value);
	}
}
