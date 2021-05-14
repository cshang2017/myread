

package org.apache.flink.table.expressions;

import org.apache.flink.annotation.PublicEvolving;

/**
 * Units for working with points in time.
 */
@PublicEvolving
public enum TimePointUnit implements TableSymbol {
	YEAR,
	MONTH,
	DAY,
	HOUR,
	MINUTE,
	SECOND,
	QUARTER,
	WEEK,
	MILLISECOND,
	MICROSECOND
}
