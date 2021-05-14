
package org.apache.flink.table.sources;

import org.apache.flink.annotation.PublicEvolving;

/**
 * Adds support for projection push-down to a {@link TableSource}.
 *
 * <p>A {@link TableSource} extending this interface is able to project the fields of the returned
 * {@code DataSet} if it is a {@code BatchTableSource} or {@code DataStream} if it is a
 * {@code StreamTableSource}.
 *
 * @param <T> The return type of the {@link TableSource}.
 */
@PublicEvolving
public interface ProjectableTableSource<T> {

	/**
	 * Creates a copy of the {@link TableSource} that projects its output to the given field indexes.
	 * The field indexes relate to the physical poduced data type ({@link TableSource#getProducedDataType()})
	 * and not to the table schema ({@link TableSource#getTableSchema} of the {@link TableSource}.
	 *
	 * <p>The table schema ({@link TableSource#getTableSchema} of the {@link TableSource} copy must not be
	 * modified by this method, but only the produced data type ({@link TableSource#getProducedDataType()})
	 * and the produced {@code DataSet} ({@code BatchTableSource#getDataSet(}) or {@code DataStream}
	 * ({@code StreamTableSource#getDataStream}).
	 *
	 * <p>If the {@link TableSource} implements the {@link DefinedFieldMapping} interface, it might
	 * be necessary to adjust the mapping as well.
	 *
	 * <p>IMPORTANT: This method must return a true copy and must not modify the original table
	 * source object.
	 *
	 * @param fields The indexes of the fields to return.
	 * @return A copy of the {@link TableSource} that projects its output.
	 */
	TableSource<T> projectFields(int[] fields);
}
