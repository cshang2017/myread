

package org.apache.flink.table.expressions;

import org.apache.flink.annotation.PublicEvolving;

/**
 * Units for working with time intervals.
 */
@PublicEvolving
public enum TimeIntervalUnit implements TableSymbol {
	YEAR,
	YEAR_TO_MONTH,
	QUARTER,
	MONTH,
	WEEK,
	DAY,
	DAY_TO_HOUR,
	DAY_TO_MINUTE,
	DAY_TO_SECOND,
	HOUR,
	SECOND,
	HOUR_TO_MINUTE,
	HOUR_TO_SECOND,
	MINUTE,
	MINUTE_TO_SECOND
}
