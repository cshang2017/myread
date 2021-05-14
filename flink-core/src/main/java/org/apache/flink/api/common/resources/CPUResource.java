package org.apache.flink.api.common.resources;

import org.apache.flink.annotation.Internal;

import java.math.BigDecimal;

/**
 * Represents CPU resource.
 */
@Internal
public class CPUResource extends Resource {


	public static final String NAME = "CPU";

	public CPUResource(double value) {
		super(NAME, value);
	}

	private CPUResource(BigDecimal value) {
		super(NAME, value);
	}

	@Override
	public Resource create(BigDecimal value) {
		return new CPUResource(value);
	}
}
