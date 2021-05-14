package org.apache.flink.table.connector.format;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.types.DataType;

/**
 * A {@link Format} for a {@link DynamicTableSource} for reading rows.
 *
 * @param <I> runtime interface needed by the table source
 */
@PublicEvolving
public interface DecodingFormat<I> extends Format {

	/**
	 * Creates runtime decoder implementation that is configured to produce data of the given data type.
	 */
	I createRuntimeDecoder(DynamicTableSource.Context context, DataType producedDataType);
}
