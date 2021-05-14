package org.apache.flink.table.data.conversion;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;

import java.util.List;
import java.util.stream.IntStream;

/**
 * Converter for {@link RowType} of {@link Row} external type.
 */
@Internal
public class RowRowConverter implements DataStructureConverter<RowData, Row> {

	private final DataStructureConverter<Object, Object>[] fieldConverters;

	private final RowData.FieldGetter[] fieldGetters;

	private RowRowConverter(
			DataStructureConverter<Object, Object>[] fieldConverters,
			RowData.FieldGetter[] fieldGetters) {
		this.fieldConverters = fieldConverters;
		this.fieldGetters = fieldGetters;
	}

	@Override
	public void open(ClassLoader classLoader) {
		for (DataStructureConverter<Object, Object> fieldConverter : fieldConverters) {
			fieldConverter.open(classLoader);
		}
	}

	@Override
	public RowData toInternal(Row external) {
		final int length = fieldConverters.length;
		final GenericRowData genericRow = new GenericRowData(external.getKind(), length);
		for (int pos = 0; pos < length; pos++) {
			final Object value = external.getField(pos);
			genericRow.setField(pos, fieldConverters[pos].toInternalOrNull(value));
		}
		return genericRow;
	}

	@Override
	public Row toExternal(RowData internal) {
		final int length = fieldConverters.length;
		final Row row = new Row(internal.getRowKind(), length);
		for (int pos = 0; pos < length; pos++) {
			final Object value = fieldGetters[pos].getFieldOrNull(internal);
			row.setField(pos, fieldConverters[pos].toExternalOrNull(value));
		}
		return row;
	}

	public static RowRowConverter create(DataType dataType) {

		final List<DataType> fields = dataType.getChildren();
		
		final DataStructureConverter<Object, Object>[] fieldConverters = fields.stream()
			.map(dt -> DataStructureConverters.getConverter(dt))
			.toArray(DataStructureConverter[]::new);
		
		final RowData.FieldGetter[] fieldGetters = IntStream
			.range(0, fields.size())
			.mapToObj(pos -> RowData.createFieldGetter(fields.get(pos).getLogicalType(), pos))
			.toArray(RowData.FieldGetter[]::new);
			
		return new RowRowConverter(fieldConverters, fieldGetters);
	}
}
