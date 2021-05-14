package org.apache.flink.table.runtime.typeutils;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.base.BigDecComparator;
import org.apache.flink.api.common.typeutils.base.BigDecSerializer;

import java.math.BigDecimal;
import java.util.Arrays;

/**
 * {@link TypeInformation} for {@link BigDecimal}.
 *
 * <p>It differs from {@link BasicTypeInfo#BIG_DEC_TYPE_INFO} in that:
 * This type includes `precision` and `scale`, similar to SQL DECIMAL.
 */
public class BigDecimalTypeInfo extends BasicTypeInfo<BigDecimal> {

	public static BigDecimalTypeInfo of(int precision, int scale) {
		return new BigDecimalTypeInfo(precision, scale);
	}

	public static BigDecimalTypeInfo of(BigDecimal value) {
		return of(value.precision(), value.scale());
	}

	private final int precision;

	private final int scale;

	public BigDecimalTypeInfo(int precision, int scale) {
		super(BigDecimal.class, new Class<?>[]{}, BigDecSerializer.INSTANCE, BigDecComparator.class);
		this.precision = precision;
		this.scale = scale;
	}

	@Override
	public String toString() {
		return String.format("Decimal(%d,%d)", precision(), scale());
	}

	@Override
	public boolean equals(Object obj) {
		if (!(obj instanceof BigDecimalTypeInfo)) {
			return false;
		}
		BigDecimalTypeInfo that = (BigDecimalTypeInfo) obj;
		return this.precision() == that.precision() && this.scale() == that.scale();
	}

	@Override
	public int hashCode() {
		int h0 = this.getClass().getCanonicalName().hashCode();
		return Arrays.hashCode(new int[]{h0, precision(), scale()});
	}

	@Override
	public boolean shouldAutocastTo(BasicTypeInfo<?> to) {
		return (to.getTypeClass() == BigDecimal.class)
			|| super.shouldAutocastTo(to);
	}

	public int precision() {
		return precision;
	}

	public int scale() {
		return scale;
	}
}

