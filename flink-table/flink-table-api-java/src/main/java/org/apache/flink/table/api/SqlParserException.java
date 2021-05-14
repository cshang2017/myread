

package org.apache.flink.table.api;

import org.apache.flink.annotation.PublicEvolving;

/**
 * Exception for all errors occurring during sql parsing.
 *
 * <p>This exception indicates that the SQL parse failed.
 */
@PublicEvolving
public class SqlParserException extends RuntimeException {

	public SqlParserException(String message, Throwable cause) {
		super(message, cause);
	}

	public SqlParserException(String message) {
		super(message);
	}
}
