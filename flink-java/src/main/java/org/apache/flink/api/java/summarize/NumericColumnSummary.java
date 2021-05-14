

package org.apache.flink.api.java.summarize;

import org.apache.flink.annotation.PublicEvolving;

/**
 * Generic Column Summary for Numeric Types.
 *
 * <p>Some values are considered "missing" where "missing" is defined as null, NaN, or Infinity.
 * These values are ignored in some calculations like mean, variance, and standardDeviation.
 *
 * <p>Uses the Kahan summation algorithm to avoid numeric instability when computing variance.
 * The algorithm is described in: "Scalable and Numerically Stable Descriptive Statistics in SystemML",
 * Tian et al, International Conference on Data Engineering 2012.
 *
 * @param <T> the numeric type e.g. Integer, Double
 */
@PublicEvolving
public class NumericColumnSummary<T> extends ColumnSummary implements java.io.Serializable {

	private final long nonMissingCount; // count of elements that are NOT null, NaN, or Infinite
	private final long nullCount;
	private final long nanCount; // always zero for types like Short, Integer, Long
	private final long infinityCount; // always zero for types like Short, Integer, Long

	private final T min;
	private final T max;
	private final T sum;

	private final Double mean;
	private final Double variance;
	private final Double standardDeviation;

	public NumericColumnSummary(long nonMissingCount, long nullCount, long nanCount, long infinityCount, T min, T max, T sum, Double mean, Double variance, Double standardDeviation) {
		this.nonMissingCount = nonMissingCount;
		this.nullCount = nullCount;
		this.nanCount = nanCount;
		this.infinityCount = infinityCount;
		this.min = min;
		this.max = max;
		this.sum = sum;
		this.mean = mean;
		this.variance = variance;
		this.standardDeviation = standardDeviation;
	}

	/**
	 * The number of "missing" values where "missing" is defined as null, NaN, or Infinity.
	 *
	 * <p>These values are ignored in some calculations like mean, variance, and standardDeviation.
	 */
	public long getMissingCount() {
		return nullCount + nanCount + infinityCount;
	}

	/**
	 * The number of values that are not null, NaN, or Infinity.
	 */
	public long getNonMissingCount() {
		return nonMissingCount;
	}

	/**
	 * The number of non-null values in this column.
	 */
	@Override
	public long getNonNullCount() {
		return nonMissingCount + nanCount + infinityCount;
	}

	@Override
	public long getNullCount() {
		return nullCount;
	}

	/**
	 * Number of values that are NaN.
	 *
	 * <p>(always zero for types like Short, Integer, Long)
	 */
	public long getNanCount() {
		return nanCount;
	}

	/**
	 * Number of values that are positive or negative infinity.
	 *
	 * <p>(always zero for types like Short, Integer, Long)
	 */
	public long getInfinityCount() {
		return infinityCount;
	}

	public T getMin() {
		return min;
	}

	public T getMax() {
		return max;
	}

	public T getSum() {
		return sum;
	}

	/**
	 * Null, NaN, and Infinite values are ignored in this calculation.
	 *
	 * @see <a href="https://en.wikipedia.org/wiki/Mean">Arithmetic Mean</a>
	 */
	public Double getMean() {
		return mean;
	}

	/**
	 * Variance is a measure of how far a set of numbers are spread out.
	 *
	 * <p>Null, NaN, and Infinite values are ignored in this calculation.
	 *
	 * @see <a href="https://en.wikipedia.org/wiki/Variance">Variance</a>
	 */
	public Double getVariance() {
		return variance;
	}

	/**
	 * Standard Deviation is a measure of variation in a set of numbers.  It is the square root of the variance.
	 *
	 * <p>Null, NaN, and Infinite values are ignored in this calculation.
	 *
	 * @see <a href="https://en.wikipedia.org/wiki/Standard_deviation">Standard Deviation</a>
	 */
	public Double getStandardDeviation() {
		return standardDeviation;
	}

	@Override
	public String toString() {
		return "NumericColumnSummary{" +
			"totalCount=" + getTotalCount() +
			", nullCount=" + nullCount +
			", nonNullCount=" + getNonNullCount() +
			", missingCount=" + getMissingCount() +
			", nonMissingCount=" + nonMissingCount +
			", nanCount=" + nanCount +
			", infinityCount=" + infinityCount +
			", min=" + min +
			", max=" + max +
			", sum=" + sum +
			", mean=" + mean +
			", variance=" + variance +
			", standardDeviation=" + standardDeviation +
			'}';
	}
}
