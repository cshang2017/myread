

package org.apache.flink.table.api;

import org.apache.flink.annotation.PublicEvolving;

/**
 * Enumeration of valid SQL compatibility modes.
 *
 * <p>In most of the cases, the built-in compatibility mode should be sufficient. For some features,
 * i.e. the "INSERT INTO T PARTITION(a='xxx') ..." grammar, you may need to switch to the Hive dialect
 * if required.
 *
 * <p>We may introduce other SQL dialects in the future.
 */
@PublicEvolving
public enum SqlDialect {

	/**
	 * Flink's default SQL behavior.
	 */
	DEFAULT,

	/**
	 * SQL dialect that allows some Apache Hive specific grammar.
	 *
	 * <p>Note: We might never support all of the Hive grammar. See the documentation for supported
	 * features.
	 */
	HIVE
}
