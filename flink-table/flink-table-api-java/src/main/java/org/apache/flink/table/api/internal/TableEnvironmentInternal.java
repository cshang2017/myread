package org.apache.flink.table.api.internal;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.ExplainDetail;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.delegation.Parser;
import org.apache.flink.table.operations.ModifyOperation;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.QueryOperation;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.sources.TableSource;

import java.util.List;

/**
 * An internal interface of {@link TableEnvironment}
 * that defines extended methods used for {@link TableImpl}.
 *
 * <p>Once old planner is removed, this class also can be removed.
 * By then, these methods can be moved into TableEnvironmentImpl.
 */
@Internal
public interface TableEnvironmentInternal extends TableEnvironment {

	/**
	 * Return a {@link Parser} that provides methods for parsing a SQL string.
	 *
	 * @return initialized {@link Parser}.
	 */
	Parser getParser();

	/**
	 * Returns a {@link CatalogManager} that deals with all catalog objects.
	 */
	CatalogManager getCatalogManager();

	/**
	 * Execute the given modify operations and return the execution result.
	 *
	 * @param operations The operations to be executed.
	 * @return the affected row counts (-1 means unknown).
	 */
	TableResult executeInternal(List<ModifyOperation> operations);

	/**
	 * Execute the given query operation and return the execution result.
	 *
	 * @param operation The QueryOperation to be executed.
	 * @return the content of the QueryOperation.
	 */
	TableResult executeInternal(QueryOperation operation);

	/**
	 * Returns the AST of this table and the execution plan to compute
	 * the result of this table.
	 *
	 * @param operations The operations to be explained.
	 * @param extraDetails The extra explain details which the explain result should include,
	 *   e.g. estimated cost, changelog mode for streaming
	 * @return AST and the execution plan.
	 */
	String explainInternal(List<Operation> operations, ExplainDetail... extraDetails);


	/**
	 * Registers an external {@link TableSource} in this {@link TableEnvironment}'s catalog.
	 * Registered tables can be referenced in SQL queries.
	 *
	 * <p>Temporary objects can shadow permanent ones. If a permanent object in a given path exists, it will
	 * be inaccessible in the current session. To make the permanent object available again one can drop the
	 * corresponding temporary object.
	 *
	 * @param name        The name under which the {@link TableSource} is registered.
	 * @param tableSource The {@link TableSource} to register.
	 */
	void registerTableSourceInternal(String name, TableSource<?> tableSource);

	/**
	 * Registers an external {@link TableSink} with already configured field names and field types in
	 * this {@link TableEnvironment}'s catalog.
	 * Registered sink tables can be referenced in SQL DML statements.
	 *
	 * <p>Temporary objects can shadow permanent ones. If a permanent object in a given path exists, it will
	 * be inaccessible in the current session. To make the permanent object available again one can drop the
	 * corresponding temporary object.
	 *
	 * @param name The name under which the {@link TableSink} is registered.
	 * @param configuredSink The configured {@link TableSink} to register.
	 */
	void registerTableSinkInternal(String name, TableSink<?> configuredSink);
}
