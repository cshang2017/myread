package org.apache.flink.table.data.conversion;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.runtime.functions.SqlDateTimeUtils;
import org.apache.flink.table.types.logical.TimeType;

/**
 * Converter for {@link TimeType} of {@link java.sql.Time} external type.
 */
@Internal
public class TimeTimeConverter implements DataStructureConverter<Integer, java.sql.Time> {


	@Override
	public Integer toInternal(java.sql.Time external) {
		return SqlDateTimeUtils.timeToInternal(external);
	}

	@Override
	public java.sql.Time toExternal(Integer internal) {
		return SqlDateTimeUtils.internalToTime(internal);
	}
}
