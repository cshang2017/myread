package org.apache.flink.table.planner.plan.schema;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.types.logical.RawType;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.AbstractSqlType;
import org.apache.calcite.sql.type.SqlTypeName;

/**
 * The {@link RelDataType} representation of a {@link RawType}.
 */
@Internal
public final class RawRelDataType extends AbstractSqlType {

	private final RawType<?> rawType;

	public RawRelDataType(RawType<?> rawType) {
		super(SqlTypeName.OTHER, rawType.isNullable(), null);
		this.rawType = rawType;
		computeDigest();
	}

	public RawType<?> getRawType() {
		return rawType;
	}

	public RawRelDataType createWithNullability(boolean nullable) {
		if (nullable == isNullable()) {
			return this;
		}
		return new RawRelDataType((RawType<?>) rawType.copy(nullable));
	}

	@Override
	protected void generateTypeString(StringBuilder sb, boolean withDetail) {
		if (withDetail) {
			sb.append(rawType.asSerializableString());
		} else {
			sb.append(rawType.asSummaryString());
		}
	}

	@Override
	protected void computeDigest() {
		final StringBuilder sb = new StringBuilder();
		generateTypeString(sb, true);
		digest = sb.toString();
	}
}
