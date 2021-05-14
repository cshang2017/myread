package org.apache.flink.table.api;

import org.apache.flink.annotation.PublicEvolving;

/**
 * ResultKind defines the types of the result.
 */
@PublicEvolving
public enum ResultKind {
	/**
	 * The statement (e.g. DDL, USE) executes successfully,
	 * and the result only contains a simple "OK".
	 */
	SUCCESS,

	/**
	 * The statement (e.g. DML, DQL, SHOW) executes successfully,
	 * and the result contains important content.
	 */
	SUCCESS_WITH_CONTENT
}
