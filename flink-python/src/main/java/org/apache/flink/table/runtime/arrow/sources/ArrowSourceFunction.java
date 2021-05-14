package org.apache.flink.table.runtime.arrow.sources;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.arrow.ArrowReader;
import org.apache.flink.table.runtime.arrow.ArrowUtils;
import org.apache.flink.table.runtime.types.TypeInfoDataTypeConverter;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;

import org.apache.arrow.vector.VectorSchemaRoot;

/**
 * An Arrow {@link SourceFunction} which takes {@link RowData} as the type of the produced records.
 */
@Internal
public class ArrowSourceFunction extends AbstractArrowSourceFunction<RowData> {


	ArrowSourceFunction(DataType dataType, byte[][] arrowData) {
		super(dataType, arrowData);
	}

	@Override
	ArrowReader<RowData> createArrowReader(VectorSchemaRoot root) {
		return ArrowUtils.createRowDataArrowReader(root, (RowType) dataType.getLogicalType());
	}

	@Override
	public TypeInformation<RowData> getProducedType() {
		return (TypeInformation<RowData>) TypeInfoDataTypeConverter.fromDataTypeToTypeInfo(dataType);
	}
}
