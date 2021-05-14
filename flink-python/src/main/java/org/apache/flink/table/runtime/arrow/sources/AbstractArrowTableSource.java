
package org.apache.flink.table.runtime.arrow.sources;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.utils.DataTypeUtils;

/**
 * A {@link StreamTableSource} for serialized arrow record batch data.
 */
@Internal
public abstract class AbstractArrowTableSource<T> implements StreamTableSource<T> {

	final DataType dataType;
	final byte[][] arrowData;

	AbstractArrowTableSource(DataType dataType, byte[][] arrowData) {
		this.dataType = dataType;
		this.arrowData = arrowData;
	}

	@Override
	public boolean isBounded() {
		return true;
	}

	@Override
	public TableSchema getTableSchema() {
		return DataTypeUtils.expandCompositeTypeToSchema(dataType);
	}

	@Override
	public DataType getProducedDataType() {
		return dataType;
	}
}
