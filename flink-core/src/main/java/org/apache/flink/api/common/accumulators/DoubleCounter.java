package org.apache.flink.api.common.accumulators;

import org.apache.flink.annotation.PublicEvolving;

/**
 * An accumulator that sums up {@code double} values.
 */
@PublicEvolving
public class DoubleCounter implements SimpleAccumulator<Double> {

	private double localValue = 0;

	public DoubleCounter() {}

	public DoubleCounter(double value) {
		this.localValue = value;
	}

	// ------------------------------------------------------------------------
	//  Accumulator
	// ------------------------------------------------------------------------

	/**
	 * Consider using {@link #add(double)} instead for primitive double values
	 */
	@Override
	public void add(Double value) {
		localValue += value;
	}

	@Override
	public Double getLocalValue() {
		return localValue;
	}

	@Override
	public void merge(Accumulator<Double, Double> other) {
		this.localValue += other.getLocalValue();
	}

	@Override
	public void resetLocal() {
		this.localValue = 0;
	}

	@Override
	public DoubleCounter clone() {
		DoubleCounter result = new DoubleCounter();
		result.localValue = localValue;
		return result;
	}

	// ------------------------------------------------------------------------
	//  Primitive Specializations
	// ------------------------------------------------------------------------

	public void add(double value){
		localValue += value;
	}

	public double getLocalValuePrimitive() {
		return this.localValue;
	}

	// ------------------------------------------------------------------------
	//  Utilities
	// ------------------------------------------------------------------------

	@Override
	public String toString() {
		return "DoubleCounter " + this.localValue;
	}
}
