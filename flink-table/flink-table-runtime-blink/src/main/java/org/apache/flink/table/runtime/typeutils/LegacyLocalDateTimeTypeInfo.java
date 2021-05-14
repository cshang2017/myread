package org.apache.flink.table.runtime.typeutils;

import org.apache.flink.api.common.typeinfo.LocalTimeTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.base.LocalDateTimeComparator;
import org.apache.flink.api.common.typeutils.base.LocalDateTimeSerializer;

import java.time.LocalDateTime;
import java.util.Objects;

/**
 * {@link TypeInformation} for {@link LocalDateTime}.
 *
 * <p>The difference between Types.LOCAL_DATE_TIME is this TypeInformation holds a precision
 * Reminder: Conversion from DateType to TypeInformation (and back) exists in
 * TableSourceUtil.computeIndexMapping, which should be fixed after we remove Legacy TypeInformation
 * TODO: https://issues.apache.org/jira/browse/FLINK-14927
 */
public class LegacyLocalDateTimeTypeInfo extends LocalTimeTypeInfo<LocalDateTime> {

	private final int precision;

	public LegacyLocalDateTimeTypeInfo(int precision) {
		super(
			LocalDateTime.class,
			LocalDateTimeSerializer.INSTANCE,
			LocalDateTimeComparator.class);
		this.precision = precision;
	}

	@Override
	public boolean equals(Object obj) {
		if (!(obj instanceof LegacyLocalDateTimeTypeInfo)) {
			return false;
		}
		LegacyLocalDateTimeTypeInfo that = (LegacyLocalDateTimeTypeInfo) obj;
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
