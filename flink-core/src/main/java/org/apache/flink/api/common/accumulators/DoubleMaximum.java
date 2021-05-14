package org.apache.flink.api.common.accumulators;

import org.apache.flink.annotation.PublicEvolving;

/**
 * An accumulator that finds the maximum {@code double} value.
 */
@PublicEvolving
public class DoubleMaximum implements SimpleAccumulator<Double> {

	private static final long serialVersionUID = 1L;

	private double max = Double.NEGATIVE_INFINITY;

	public DoubleMaximum() {}

	public DoubleMaximum(double value) {
		this.max = value;
	}

	// ------------------------------------------------------------------------
	//  Accumulator
	// ------------------------------------------------------------------------

	/**
	 * Consider using {@link #add(double)} instead for primitive double values
	 */
	@Override
	public void add(Double value) {
		this.max = Math.max(this.max, value);
	}

	@Override
	public Double getLocalValue() {
		return this.max;
	}

	@Override
	public void merge(Accumulator<Double, Double> other) {
		this.max = Math.max(this.max, other.getLocalValue());
	}

	@Override
	public void resetLocal() {
		this.max = Double.NEGATIVE_INFINITY;
	}

	@Override
	public DoubleMaximum clone() {
		DoubleMaximum clone = new DoubleMaximum();
		clone.max = this.max;
		return clone;
	}

	// ------------------------------------------------------------------------
	//  Primitive Specializations
	// ------------------------------------------------------------------------

	public void add(double value) {
		this.max = Math.max(this.max, value);
	}

	public double getLocalValuePrimitive() {
		return this.max;
	}

	// ------------------------------------------------------------------------
	//  Utilities
	// ------------------------------------------------------------------------

	@Override
	public String toString() {
		return "DoubleMaximum " + this.max;
	}
}
