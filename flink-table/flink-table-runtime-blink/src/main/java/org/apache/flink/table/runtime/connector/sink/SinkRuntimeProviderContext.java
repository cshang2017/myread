package org.apache.flink.table.runtime.connector.sink;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.data.conversion.DataStructureConverters;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.TypeTransformations;
import org.apache.flink.table.types.utils.DataTypeUtils;

import static org.apache.flink.table.runtime.types.TypeInfoDataTypeConverter.fromDataTypeToTypeInfo;

/**
 * Implementation of {@link DynamicTableSink.Context}.
 */
@Internal
public final class SinkRuntimeProviderContext implements DynamicTableSink.Context {

	private final boolean isBounded;

	public SinkRuntimeProviderContext(boolean isBounded) {
		this.isBounded = isBounded;
	}

	@Override
	public boolean isBounded() {
		return isBounded;
	}

	@Override
	public TypeInformation<?> createTypeInformation(DataType consumedDataType) {
		final DataType internalDataType = DataTypeUtils.transform(
			consumedDataType,
			TypeTransformations.TO_INTERNAL_CLASS);
		return fromDataTypeToTypeInfo(internalDataType);
	}

	@Override
	public DynamicTableSink.DataStructureConverter createDataStructureConverter(DataType consumedDataType) {
		return new DataStructureConverterWrapper(DataStructureConverters.getConverter(consumedDataType));
	}
}
