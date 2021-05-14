package org.apache.flink.api.java.summarize.aggregation;

import org.apache.flink.annotation.Internal;

/**
 * Used to calculate sums using the Kahan summation algorithm.
 *
 * <p>The Kahan summation algorithm (also known as compensated summation) reduces the numerical errors that
 * occur when adding a sequence of finite precision floating point numbers. Numerical errors arise due to
 * truncation and rounding. These errors can lead to numerical instability.
 *
 * @see <a href="http://en.wikipedia.org/wiki/Kahan_summation_algorithm">Kahan Summation Algorithm</a>
 */
@Internal
public class CompensatedSum implements java.io.Serializable {

	private static final double NO_CORRECTION = 0.0;
	public static final CompensatedSum ZERO = new CompensatedSum(0.0, NO_CORRECTION);

	private static final long serialVersionUID = 1L;

	private final double value;
	private final double delta;

	/**
	 * Used to calculate sums using the Kahan summation algorithm.
	 * @param value the sum
	 * @param delta correction term
	 */
	public CompensatedSum(double value, double delta) {
		this.value = value;
		this.delta = delta;
	}

	/**
	 * The value of the sum.
	 */
	public double value() {
		return value;
	}

	/**
	 * The correction term.
	 */
	public double delta() {
		return delta;
	}

	/**
	 * Increments the Kahan sum by adding a value and a correction term.
	 */
	public CompensatedSum add(double value, double delta) {
		return add(new CompensatedSum(value, delta));
	}

	/**
	 * Increments the Kahan sum by adding a value without a correction term.
	 */
	public CompensatedSum add(double value) {
		return add(new CompensatedSum(value, NO_CORRECTION));
	}

	/**
	 * Increments the Kahan sum by adding two sums, and updating the correction term for reducing numeric errors.
	 */
	public CompensatedSum add(CompensatedSum other) {
		double correctedSum = other.value() + (delta + other.delta());
		double updatedValue = value + correctedSum;
		double updatedDelta = correctedSum - (updatedValue - value);
		return new CompensatedSum(updatedValue, updatedDelta);
	}

}

