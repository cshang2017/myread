package org.apache.flink.table.api;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

import java.util.Optional;

/**
 * A TableResult is the representation of the statement execution result.
 */
@PublicEvolving
public interface TableResult {

	/**
	 * For DML and DQL statement, return the {@link JobClient}
	 * which associates the submitted Flink job.
	 * For other statements (e.g.  DDL, DCL) return empty.
	 */
	Optional<JobClient> getJobClient();

	/**
	 * Get the schema of result.
	 *
	 * <p>The schema of DDL, USE, EXPLAIN:
	 * <pre>
	 * +-------------+-------------+----------+
	 * | column name | column type | comments |
	 * +-------------+-------------+----------+
	 * | result      | STRING      |          |
	 * +-------------+-------------+----------+
	 * </pre>
	 *
	 * <p>The schema of SHOW:
	 * <pre>
	 * +---------------+-------------+----------+
	 * |  column name  | column type | comments |
	 * +---------------+-------------+----------+
	 * | &lt;object name&gt; | STRING      |          |
	 * +---------------+-------------+----------+
	 * The column name of `SHOW CATALOGS` is "catalog name",
	 * the column name of `SHOW DATABASES` is "database name",
	 * the column name of `SHOW TABLES` is "table name",
	 * the column name of `SHOW VIEWS` is "view name",
	 * the column name of `SHOW FUNCTIONS` is "function name".
	 * </pre>
	 *
	 * <p>The schema of DESCRIBE:
	 * <pre>
	 * +------------------+-------------+-----------------------------------------------------------------------------+
	 * | column name      | column type |                              comments                                       |
	 * +------------------+-------------+-----------------------------------------------------------------------------+
	 * | name             | STRING      | field name                                                                  |
	 * | type             | STRING      | field type expressed as a String                                            |
	 * | null             | BOOLEAN     | field nullability: true if a field is nullable, else false                  |
	 * | key              | BOOLEAN     | key constraint: 'PRI' for primary keys, 'UNQ' for unique keys, else null    |
	 * | computed column  | STRING      | computed column: string expression if a field is computed column, else null |
	 * | watermark        | STRING      | watermark: string expression if a field is watermark, else null             |
	 * +------------------+-------------+-----------------------------------------------------------------------------+
	 * </pre>
	 *
	 * <p>The schema of INSERT: (one column per one sink)
	 * <pre>
	 * +----------------------------+-------------+-----------------------+
	 * | column name                | column type | comments              |
	 * +----------------------------+-------------+-----------------------+
	 * | (name of the insert table) | BIGINT      | the insert table name |
	 * +----------------------------+-------------+-----------------------+
	 * </pre>
	 *
	 * <p>The schema of SELECT is the selected field names and types.
	 */
	TableSchema getTableSchema();

	/**
	 * Return the {@link ResultKind} which represents the result type.
	 *
	 * <p>For DDL operation and USE operation, the result kind is always {@link ResultKind#SUCCESS}.
	 * For other operations, the result kind is always {@link ResultKind#SUCCESS_WITH_CONTENT}.
	 */
	ResultKind getResultKind();

	/**
	 * Get the result contents as a closeable row iterator.
	 *
	 * <p><strong>NOTE:</strong>
	 * <ul>
	 *     <li>
	 *         For SELECT operation, the job will not be finished unless all result data has been collected.
	 *         So we should actively close the job to avoid resource leak through CloseableIterator#close method.
	 *         Calling CloseableIterator#close method will cancel the job and release related resources.
	 *     </li>
	 *     <li>
	 *         For DML operation, Flink does not support getting the real affected row count now.
	 *         So the affected row count is always -1 (unknown) for every sink, and them will be
	 *         returned after the job is submitted.
	 *         Calling CloseableIterator#close method does not bind to the job.
	 *         Therefore the `CloseableIterator#close` will not cancel the job as in the case of SELECT.
	 *         If you need to cancel the job, you can use the {@link #getJobClient()}.
	 *     </li>
	 *     <li>
	 *         For other operations, no flink job will be submitted ({@link #getJobClient()} is always empty),
	 *         and the result is bounded. Do nothing when calling CloseableIterator#close method.
	 *     </li>
	 * </ul>
	 *
	 * <p>Recommended code to call CloseableIterator#close method looks like:
	 * <pre>{@code
	 *  TableResult result = tEnv.execute("select ...");
	 *  // using try-with-resources statement
	 *  try (CloseableIterator<Row> it = result.collect()) {
	 *      it... // collect same data
	 *  }
	 * }</pre>
	 *
	 * <p>For streaming mode, this method guarantees end-to-end exactly-once record delivery
	 * which requires the checkpointing mechanism to be enabled.
	 * By default, checkpointing is disabled. To enable checkpointing, set checkpointing properties
	 * (see ExecutionCheckpointingOptions) through {@link TableConfig#getConfiguration()}.
	 *
	 * <p>In order to fetch result to local, you can call either {@link #collect()} and {@link #print()}.
	 * But, they can't be called both on the same {@link TableResult} instance,
	 * because the result can only be accessed once.
	 */
	CloseableIterator<Row> collect();

	/**
	 * Print the result contents as tableau form to client console.
	 *
	 * <p>For streaming mode, this method guarantees end-to-end exactly-once record delivery
	 * ! which requires the checkpointing mechanism to be enabled.
	 * READ : By default, checkpointing is disabled. To enable checkpointing, set checkpointing properties
	 * (see ExecutionCheckpointingOptions) through {@link TableConfig#getConfiguration()}.
	 *
	 * <p>In order to fetch result to local, you can call either {@link #collect()} and {@link #print()}.
	 * But, they can't be called both on the same {@link TableResult} instance,
	 * because the result can only be accessed once.
	 */
	void print();
}
