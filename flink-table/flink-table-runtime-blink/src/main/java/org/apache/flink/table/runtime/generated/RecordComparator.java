package org.apache.flink.table.runtime.generated;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.operators.sort.BinaryInMemorySortBuffer;

import java.io.Serializable;
import java.util.Comparator;

/**
 * Record comparator for {@link BinaryInMemorySortBuffer}.
 * For performance, subclasses are usually implemented through CodeGenerator.
 * A new interface for helping JVM inline.
 */
public interface RecordComparator extends Comparator<RowData>, Serializable {

	@Override
	int compare(RowData o1, RowData o2);
}
