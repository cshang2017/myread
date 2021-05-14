package org.apache.flink.table.planner.operations;

/**
 * Exception thrown during the execution of SQL statements.
 */
public class SqlConversionException extends RuntimeException {

	private static final long serialVersionUID = 1L;

	public SqlConversionException(String message) {
		super(message);
	}

	public SqlConversionException(String message, Throwable e) {
		super(message, e);
	}
}
