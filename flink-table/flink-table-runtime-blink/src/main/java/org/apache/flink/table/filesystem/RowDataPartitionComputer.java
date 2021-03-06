
package org.apache.flink.table.filesystem;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * {@link PartitionComputer} for {@link RowData}.
 */
@Internal
public class RowDataPartitionComputer implements PartitionComputer<RowData> {

	protected final String defaultPartValue;
	protected final String[] partitionColumns;
	protected final int[] partitionIndexes;
	protected final LogicalType[] partitionTypes;

	private final int[] nonPartitionIndexes;
	private final LogicalType[] nonPartitionTypes;

	private transient GenericRowData reuseRow;

	public RowDataPartitionComputer(
			String defaultPartValue,
			String[] columnNames,
			DataType[] columnTypes,
			String[] partitionColumns) {
		this.defaultPartValue = defaultPartValue;
		this.partitionColumns = partitionColumns;

		List<String> columnList = Arrays.asList(columnNames);
		List<LogicalType> columnTypeList = Arrays.stream(columnTypes)
				.map(DataType::getLogicalType)
				.collect(Collectors.toList());

		this.partitionIndexes = Arrays.stream(partitionColumns)
				.mapToInt(columnList::indexOf)
				.toArray();
		this.partitionTypes = Arrays.stream(partitionIndexes)
				.mapToObj(columnTypeList::get)
				.toArray(LogicalType[]::new);

		List<Integer> partitionIndexList = Arrays.stream(partitionIndexes).boxed().collect(Collectors.toList());
		this.nonPartitionIndexes = IntStream.range(0, columnNames.length)
				.filter(c -> !partitionIndexList.contains(c))
				.toArray();
		this.nonPartitionTypes = Arrays.stream(nonPartitionIndexes)
				.mapToObj(columnTypeList::get)
				.toArray(LogicalType[]::new);
	}

	@Override
	public LinkedHashMap<String, String> generatePartValues(RowData in) {
		LinkedHashMap<String, String> partSpec = new LinkedHashMap<>();

		for (int i = 0; i < partitionIndexes.length; i++) {
			Object field = RowData.get(in, partitionIndexes[i], partitionTypes[i]);
			String partitionValue = field != null ? field.toString() : null;
			if (partitionValue == null || "".equals(partitionValue)) {
				partitionValue = defaultPartValue;
			}
			partSpec.put(partitionColumns[i], partitionValue);
		}
		return partSpec;
	}

	@Override
	public RowData projectColumnsToWrite(RowData in) {
		if (partitionIndexes.length == 0) {
			return in;
		}

		if (reuseRow == null) {
			this.reuseRow = new GenericRowData(nonPartitionIndexes.length);
		}

		for (int i = 0; i < nonPartitionIndexes.length; i++) {
			reuseRow.setField(i, RowData.get(
					in, nonPartitionIndexes[i], nonPartitionTypes[i]));
		}
		return reuseRow;
	}
}
