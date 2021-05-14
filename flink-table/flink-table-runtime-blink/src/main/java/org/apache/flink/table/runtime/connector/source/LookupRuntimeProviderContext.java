package org.apache.flink.table.runtime.connector.source;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.connector.source.DynamicTableSource.DataStructureConverter;
import org.apache.flink.table.connector.source.LookupTableSource;
import org.apache.flink.table.data.conversion.DataStructureConverters;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.TypeTransformations;
import org.apache.flink.table.types.utils.DataTypeUtils;

import static org.apache.flink.table.runtime.types.TypeInfoDataTypeConverter.fromDataTypeToTypeInfo;

/**
 * Implementation of {@link LookupTableSource.Context}.
 */
@Internal
public final class LookupRuntimeProviderContext implements LookupTableSource.LookupContext {

	private final int[][] lookupKeys;

	public LookupRuntimeProviderContext(int[][] lookupKeys) {
		this.lookupKeys = lookupKeys;
	}

	@Override
	public int[][] getKeys() {
		return lookupKeys;
	}

	@Override
	public TypeInformation<?> createTypeInformation(DataType producedDataType) {
		final DataType internalDataType = DataTypeUtils.transform(
			producedDataType,
			TypeTransformations.TO_INTERNAL_CLASS);
		return fromDataTypeToTypeInfo(internalDataType);
	}

	@Override
	public DataStructureConverter createDataStructureConverter(DataType producedDataType) {
		return new DataStructureConverterWrapper(DataStructureConverters.getConverter(producedDataType));
	}
}
