package org.apache.flink.table.api;

import org.apache.flink.annotation.PublicEvolving;

/**
 * A {@link StatementSet} accepts DML statements or {@link Table}s,
 * the planner can optimize all added statements and Tables together
 * and then submit as one job.
 *
 * <p>The added statements and Tables will be cleared
 * when calling the `execute` method.
 */
@PublicEvolving
public interface StatementSet {

	/**
	 * add insert statement to the set.
	 */
	StatementSet addInsertSql(String statement);

	/**
	 * add Table with the given sink table name to the set.
	 */
	StatementSet addInsert(String targetPath, Table table);

	/**
	 * add {@link Table} with the given sink table name to the set.
	 */
	StatementSet addInsert(String targetPath, Table table, boolean overwrite);

	/**
	 * returns the AST and the execution plan to compute the result of the
	 * all statements and Tables.
	 *
	 * @param extraDetails The extra explain details which the explain result should include,
	 *                     e.g. estimated cost, changelog mode for streaming
	 * @return AST and the execution plan.
	 */
	String explain(ExplainDetail... extraDetails);

	/**
	 * execute all statements and Tables as a batch.
	 *
	 * <p>The added statements and Tables will be cleared when executing this method.
	 */
	TableResult execute();
}
