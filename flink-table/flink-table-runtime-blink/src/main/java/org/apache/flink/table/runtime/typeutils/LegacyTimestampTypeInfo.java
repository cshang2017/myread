package org.apache.flink.table.runtime.typeutils;

import org.apache.flink.api.common.typeinfo.SqlTimeTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.base.SqlTimestampComparator;
import org.apache.flink.api.common.typeutils.base.SqlTimestampSerializer;

import java.sql.Timestamp;
import java.util.Objects;

/**
 * {@link TypeInformation} for {@link Timestamp}.
 *
 * <p>The difference between Types.SQL_TIMESTAMP is this TypeInformation holds a precision
 * Reminder: Conversion from DateType to TypeInformation (and back) exists in
 * TableSourceUtil.computeIndexMapping, which should be fixed after we remove Legacy TypeInformation
 * TODO: https://issues.apache.org/jira/browse/FLINK-14927
 */
public class LegacyTimestampTypeInfo extends SqlTimeTypeInfo<Timestamp> {

	private final int precision;

	@SuppressWarnings("unchecked")
	public LegacyTimestampTypeInfo(int precision) {
		super(
			Timestamp.class,
			SqlTimestampSerializer.INSTANCE,
			(Class) SqlTimestampComparator.class);
		this.precision = precision;
	}

	@Override
	public boolean equals(Object obj) {
		if (!(obj instanceof LegacyTimestampTypeInfo)) {
			return false;
		}
		LegacyTimestampTypeInfo that = (LegacyTimestampTypeInfo) obj;
		return this.precision == that.precision;
	}

	@Override
	public String toString() {
		return String.format("Timestamp(%d)", precision);
	}

	@Override
	public int hashCode() {
		return Objects.hash(this.getClass().getCanonicalName(), precision);
	}

	public int getPrecision() {
		return precision;
	}
}
