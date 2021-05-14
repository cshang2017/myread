package org.apache.flink.table.runtime.arrow.readers;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.runtime.functions.SqlDateTimeUtils;

import org.apache.arrow.vector.DateDayVector;

import java.sql.Date;

/**
 * {@link ArrowFieldReader} for Date.
 */
@Internal
public final class DateFieldReader extends ArrowFieldReader<Date> {

	public DateFieldReader(DateDayVector dateDayVector) {
		super(dateDayVector);
	}

	@Override
	public Date read(int index) {
		if (getValueVector().isNull(index)) {
			return null;
		} else {
			return SqlDateTimeUtils.internalToDate(((DateDayVector) getValueVector()).get(index));
		}
	}
}
