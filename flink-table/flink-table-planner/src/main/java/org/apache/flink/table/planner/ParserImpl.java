package org.apache.flink.table.planner;

import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.calcite.CalciteParser;
import org.apache.flink.table.calcite.FlinkPlannerImpl;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.catalog.UnresolvedIdentifier;
import org.apache.flink.table.delegation.Parser;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.sqlexec.SqlToOperationConverter;

import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;

import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;

/**
 * Implementation of {@link Parser} that uses Calcite.
 */
public class ParserImpl implements Parser {

	private final CatalogManager catalogManager;

	// we use supplier pattern here in order to use the most up to
	// date configuration. Users might change the parser configuration in a TableConfig in between
	// multiple statements parsing
	private final Supplier<FlinkPlannerImpl> validatorSupplier;
	private final Supplier<CalciteParser> calciteParserSupplier;

	public ParserImpl(
			CatalogManager catalogManager,
			Supplier<FlinkPlannerImpl> validatorSupplier,
			Supplier<CalciteParser> calciteParserSupplier) {
		this.catalogManager = catalogManager;
		this.validatorSupplier = validatorSupplier;
		this.calciteParserSupplier = calciteParserSupplier;
	}

	@Override
	public List<Operation> parse(String statement) {
		CalciteParser parser = calciteParserSupplier.get();
		FlinkPlannerImpl planner = validatorSupplier.get();
		// parse the sql query
		SqlNode parsed = parser.parse(statement);

		Operation operation = SqlToOperationConverter.convert(planner, catalogManager, parsed)
			.orElseThrow(() -> new TableException(
				"Unsupported SQL query! parse() only accepts SQL queries of type " +
					"SELECT, UNION, INTERSECT, EXCEPT, VALUES, ORDER_BY or INSERT;" +
					"and SQL DDLs of type " +
					"CREATE TABLE"));
		return Collections.singletonList(operation);
	}

	@Override
	public UnresolvedIdentifier parseIdentifier(String identifier) {
		CalciteParser parser = calciteParserSupplier.get();
		SqlIdentifier sqlIdentifier = parser.parseIdentifier(identifier);
		return UnresolvedIdentifier.of(sqlIdentifier.names);
	}

	@Override
	public ResolvedExpression parseSqlExpression(String sqlExpression, TableSchema inputSchema) {
		throw new UnsupportedOperationException("Computed columns is only supported by the Blink planner.");
	}
}
