package org.apache.flink.table.runtime.operators.match;

import org.apache.flink.cep.EventComparator;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.generated.GeneratedRecordComparator;
import org.apache.flink.table.runtime.generated.RecordComparator;

/**
 * An implementation of {@link EventComparator} based on a generated {@link RecordComparator}.
 */
public class RowDataEventComparator implements EventComparator<RowData> {
	private static final long serialVersionUID = 1L;

	private final GeneratedRecordComparator generatedComparator;

	private transient RecordComparator comparator;

	public RowDataEventComparator(GeneratedRecordComparator generatedComparator) {
		this.generatedComparator = generatedComparator;
	}

	@Override
	public int compare(RowData row1, RowData row2) {
		if (comparator == null) {
			comparator = generatedComparator.newInstance(
				Thread.currentThread().getContextClassLoader());
		}
		return comparator.compare(row1, row2);
	}
}
