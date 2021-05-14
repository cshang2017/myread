package org.apache.flink.table.runtime.connector.source;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.connector.RuntimeConverter;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.conversion.DataStructureConverter;

import javax.annotation.Nullable;

/**
 * Implementation of {@link DynamicTableSource.DataStructureConverter}.
 *
 * <p>It wraps the internal {@link DataStructureConverter}.
 */
@Internal
class DataStructureConverterWrapper implements DynamicTableSource.DataStructureConverter {

	private final DataStructureConverter<Object, Object> structureConverter;

	DataStructureConverterWrapper(DataStructureConverter<Object, Object> structureConverter) {
		this.structureConverter = structureConverter;
	}

	@Override
	public void open(RuntimeConverter.Context context) {
		structureConverter.open(context.getClassLoader());
	}

	@Override
	public @Nullable Object toInternal(@Nullable Object externalStructure) {
		return structureConverter.toInternalOrNull(externalStructure);
	}
}
