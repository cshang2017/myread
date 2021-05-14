

package org.apache.flink.table.connector.format;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.types.DataType;

/**
 * A {@link Format} for a {@link DynamicTableSink} for writing rows.
 *
 * @param <I> runtime interface needed by the table sink
 */
@PublicEvolving
public interface EncodingFormat<I> extends Format {

	/**
	 * Creates runtime encoder implementation that is configured to consume data of the given data type.
	 */
	I createRuntimeEncoder(DynamicTableSink.Context context, DataType consumedDataType);
}
