package org.apache.flink.table.runtime.typeutils;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.base.InstantComparator;
import org.apache.flink.api.common.typeutils.base.InstantSerializer;

import java.time.Instant;
import java.util.Objects;

/**
 * {@link TypeInformation} for {@link Instant}.
 *
 * <p>The different between Types.INSTANT is the TypeInformation holds a precision
 * Reminder: Conversion from DateType to TypeInformation (and back) exists in
 * TableSourceUtil.computeIndexMapping, which should be fixed after we remove Legacy TypeInformation
 * TODO: https://issues.apache.org/jira/browse/FLINK-14927
 */
public class LegacyInstantTypeInfo extends BasicTypeInfo<Instant> {

	private final int precision;

	public LegacyInstantTypeInfo(int precision) {
		super(
			Instant.class,
			new Class<?>[]{},
			InstantSerializer.INSTANCE,
			InstantComparator.class);
		this.precision = precision;
	}

	@Override
	public boolean equals(Object obj) {
		if (!(obj instanceof LegacyInstantTypeInfo)) {
			return false;
		}
		LegacyInstantTypeInfo that = (LegacyInstantTypeInfo) obj;
		return this.precision == that.precision;
	}

	@Override
	public String toString() {
		return String.format("TIMESTAMP(%d) WITH LOCAL TIME ZONE", precision);
	}

	@Override
	public int hashCode() {
		return Objects.hash(this.getClass().getCanonicalName(), precision);
	}

	public int getPrecision() {
		return precision;
	}
}
