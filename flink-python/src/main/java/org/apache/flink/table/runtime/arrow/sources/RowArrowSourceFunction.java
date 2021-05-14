

package org.apache.flink.table.runtime.arrow.sources;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.runtime.arrow.ArrowReader;
import org.apache.flink.table.runtime.arrow.ArrowUtils;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.types.Row;

import org.apache.arrow.vector.VectorSchemaRoot;

/**
 * An Arrow {@link SourceFunction} which takes {@link Row} as the type of the produced records.
 */
@Internal
public class RowArrowSourceFunction extends AbstractArrowSourceFunction<Row> {

	RowArrowSourceFunction(DataType dataType, byte[][] arrowData) {
		super(dataType, arrowData);
	}

	@Override
	ArrowReader<Row> createArrowReader(VectorSchemaRoot root) {
		return ArrowUtils.createRowArrowReader(root, (RowType) dataType.getLogicalType());
	}

	@Override
	public TypeInformation<Row> getProducedType() {
		return (TypeInformation<Row>) TypeConversions.fromDataTypeToLegacyInfo(dataType);
	}
}
