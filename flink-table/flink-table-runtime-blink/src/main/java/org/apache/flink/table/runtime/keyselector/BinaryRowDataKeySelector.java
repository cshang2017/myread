package org.apache.flink.table.runtime.keyselector;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.runtime.generated.GeneratedProjection;
import org.apache.flink.table.runtime.generated.Projection;
import org.apache.flink.table.runtime.typeutils.RowDataTypeInfo;

/**
 * A KeySelector which will extract key from RowData. The key type is BinaryRowData.
 */
public class BinaryRowDataKeySelector implements RowDataKeySelector {

	private final RowDataTypeInfo keyRowType;
	private final GeneratedProjection generatedProjection;
	private transient Projection<RowData, BinaryRowData> projection;

	public BinaryRowDataKeySelector(RowDataTypeInfo keyRowType, GeneratedProjection generatedProjection) {
		this.keyRowType = keyRowType;
		this.generatedProjection = generatedProjection;
	}

	@Override
	public RowData getKey(RowData value) throws Exception {
		if (projection == null) {
			ClassLoader cl = Thread.currentThread().getContextClassLoader();
			//noinspection unchecked
			projection = generatedProjection.newInstance(cl);
		}
		return projection.apply(value).copy();
	}

	@Override
	public RowDataTypeInfo getProducedType() {
		return keyRowType;
	}

}
