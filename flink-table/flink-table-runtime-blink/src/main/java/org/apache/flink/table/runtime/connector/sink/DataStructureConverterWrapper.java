

package org.apache.flink.table.runtime.connector.sink;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.data.conversion.DataStructureConverter;

import javax.annotation.Nullable;

/**
 * Implementation of {@link DynamicTableSink.DataStructureConverter}.
 *
 * <p>It wraps the internal {@link DataStructureConverter}.
 */
@Internal
class DataStructureConverterWrapper implements DynamicTableSink.DataStructureConverter {

	private final DataStructureConverter<Object, Object> structureConverter;

	DataStructureConverterWrapper(DataStructureConverter<Object, Object> structureConverter) {
		this.structureConverter = structureConverter;
	}

	@Override
	public void open(Context context) {
		structureConverter.open(context.getClassLoader());
	}

	@Override
	public @Nullable Object toExternal(@Nullable Object internalStructure) {
		return structureConverter.toExternalOrNull(internalStructure);
	}
}
