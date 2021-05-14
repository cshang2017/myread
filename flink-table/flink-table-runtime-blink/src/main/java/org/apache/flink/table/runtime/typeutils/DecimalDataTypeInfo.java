package org.apache.flink.table.runtime.typeutils;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.table.data.DecimalData;

import java.util.Arrays;

/**
 * TypeInformation for {@link DecimalData}.
 */
@Internal
public class DecimalDataTypeInfo extends TypeInformation<DecimalData> {

	public static DecimalDataTypeInfo of(int precision, int scale) {
		return new DecimalDataTypeInfo(precision, scale);
	}

	private final int precision;

	private final int scale;

	public DecimalDataTypeInfo(int precision, int scale) {
		this.precision = precision;
		this.scale = scale;
	}

	@Override
	public boolean isBasicType() {
		return true;
	}

	@Override
	public boolean isTupleType() {
		return false;
	}

	@Override
	public int getArity() {
		return 1;
	}

	@Override
	public int getTotalFields() {
		return 1;
	}

	@Override
	public Class<DecimalData> getTypeClass() {
		return DecimalData.class;
	}

	@Override
	public boolean isKeyType() {
		return true;
	}

	@Override
	public TypeSerializer<DecimalData> createSerializer(ExecutionConfig config) {
		return new DecimalDataSerializer(precision, scale);
	}

	@Override
	public String toString() {
		return String.format("Decimal(%d,%d)", precision, scale);
	}

	@Override
	public boolean equals(Object obj) {
		if (!(obj instanceof DecimalDataTypeInfo)) {
			return false;
		}
		DecimalDataTypeInfo that = (DecimalDataTypeInfo) obj;
		return this.precision == that.precision && this.scale == that.scale;
	}

	@Override
	public int hashCode() {
		int h0 = this.getClass().getCanonicalName().hashCode();
		return Arrays.hashCode(new int[]{h0, precision, scale});
	}

	@Override
	public boolean canEqual(Object obj) {
		return obj instanceof DecimalDataTypeInfo;
	}

	public int precision() {
		return precision;
	}

	public int scale() {
		return scale;
	}
}
