
package org.apache.flink.table.delegation;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.UnresolvedIdentifier;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.QueryOperation;

import java.util.List;

/**
 * Provides methods for parsing SQL objects from a SQL string.
 */
@Internal
public interface Parser {

	/**
	 * Entry point for parsing SQL queries expressed as a String.
	 *
	 * <p><b>Note:</b>If the created {@link Operation} is a {@link QueryOperation}
	 * it must be in a form that will be understood by the
	 * {@link Planner#translate(List)} method.
	 *
	 * <p>The produced Operation trees should already be validated.
	 *
	 * @param statement the SQL statement to evaluate
	 * @return parsed queries as trees of relational {@link Operation}s
	 * @throws org.apache.flink.table.api.SqlParserException when failed to parse the statement
	 */
	List<Operation> parse(String statement);

	/**
	 * Entry point for parsing SQL identifiers expressed as a String.
	 *
	 * @param identifier the SQL identifier to parse
	 * @return parsed identifier
	 * @throws org.apache.flink.table.api.SqlParserException when failed to parse the identifier
	 */
	UnresolvedIdentifier parseIdentifier(String identifier);

	/**
	 * Entry point for parsing SQL expressions expressed as a String.
	 *
	 * @param sqlExpression the SQL expression to parse
	 * @param inputSchema the schema of the fields in sql expression
	 * @return resolved expression
	 * @throws org.apache.flink.table.api.SqlParserException when failed to parse the sql expression
	 */
	ResolvedExpression parseSqlExpression(String sqlExpression, TableSchema inputSchema);
}
