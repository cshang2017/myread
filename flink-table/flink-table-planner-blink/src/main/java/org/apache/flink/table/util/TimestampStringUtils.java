

package org.apache.flink.table.util;

import org.apache.calcite.util.TimestampString;

import java.time.LocalDateTime;

/**
 * Utility functions for calcite's {@link TimestampString}.
 */
public class TimestampStringUtils {

	public static TimestampString fromLocalDateTime(LocalDateTime ldt) {
		return new TimestampString(
			ldt.getYear(),
			ldt.getMonthValue(),
			ldt.getDayOfMonth(),
			ldt.getHour(),
			ldt.getMinute(),
			ldt.getSecond()).withNanos(ldt.getNano());
	}

	public static LocalDateTime toLocalDateTime(TimestampString timestampString) {
		final String v = timestampString.toString();
		final int year = Integer.valueOf(v.substring(0, 4));
		final int month = Integer.valueOf(v.substring(5, 7));
		final int day = Integer.valueOf(v.substring(8, 10));
		final int h = Integer.valueOf(v.substring(11, 13));
		final int m = Integer.valueOf(v.substring(14, 16));
		final int s = Integer.valueOf(v.substring(17, 19));
		final int nano = getNanosInSecond(v);
		return LocalDateTime.of(year, month, day, h, m, s, nano);
	}

	private static int getNanosInSecond(String v) {
		switch (v.length()) {
			case 19: // "1999-12-31 12:34:56"
				return 0;
			default:  // "1999-12-31 12:34:56.789123456"
				return Integer.valueOf(v.substring(20))
					* (int) Math.pow(10, 9 - (v.length() - 20));
		}
	}
}
