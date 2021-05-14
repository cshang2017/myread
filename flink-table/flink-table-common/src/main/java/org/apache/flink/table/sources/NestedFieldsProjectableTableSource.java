
package org.apache.flink.table.sources;

import org.apache.flink.annotation.PublicEvolving;

/**
 * Adds support for projection push-down to a {@link TableSource} with nested fields.
 *
 * <p>A {@link TableSource} extending this interface is able to project the fields of its returned
 * {@code DataSet} if it is a {@code BatchTableSource} or {@code DataStream} if it is a
 * {@code StreamTableSource}.
 *
 * @param <T> The return type of the {@link TableSource}.
 */
@PublicEvolving
public interface NestedFieldsProjectableTableSource<T> {

	/**
	 * Creates a copy of the {@link TableSource} that projects its output to the given field indexes.
	 * The field indexes relate to the physical produced data type ({@link TableSource#getProducedDataType()})
	 * and not to the table schema ({@link TableSource#getTableSchema()} of the {@link TableSource}.
	 *
	 * <p>The table schema ({@link TableSource#getTableSchema()} of the {@link TableSource} copy must not be
	 * modified by this method, but only the produced data type ({@link TableSource#getProducedDataType()})
	 * and the produced {@code DataSet} ({@code BatchTableSource.getDataSet(}) or {@code DataStream}
	 * ({@code StreamTableSource.getDataStream()}). The produced data type may only be changed by
	 * removing or reordering first level fields. The type of the first level fields must not be
	 * changed.
	 *
	 * <p>If the {@link TableSource} implements the {@link DefinedFieldMapping} interface, it might
	 * be necessary to adjust the mapping as well.
	 *
	 * <p>The {@code nestedFields} parameter contains all nested fields that are accessed by the query.
	 * This information can be used to only read and set the accessed fields. Non-accessed fields
	 * may be left empty, set to null, or to a default value.
	 *
	 * <p>This method is called with parameters as shown in the example below:
	 *
	 * <pre>
	 * {@code
	 *
	 * // schema
	 * tableSchema = {
	 *     id,
	 *     student<\school<\city, tuition>, age, name>,
	 *     teacher<\age, name>
	 * }
	 *
	 * // query
	 * select (id, student.school.city, student.age, teacher)
	 *
	 * // parameters
	 * fields = field = [0, 1, 2]
	 * nestedFields  \[\["*"], ["school.city", "age"], ["*"\]\]
	 * }
	 * </pre>
	 *
	 * <p>IMPORTANT: This method must return a true copy and must not modify the original table source
	 * object.
	 *
	 * @param fields       The indexes of the fields to return.
	 * @param nestedFields The paths of all nested fields which are accessed by the query. All other
	 *                     nested fields may be empty.
	 * @return A copy of the {@link TableSource} that projects its output.
	 */
	TableSource<T> projectNestedFields(
		int[] fields,
		String[][] nestedFields);
}
