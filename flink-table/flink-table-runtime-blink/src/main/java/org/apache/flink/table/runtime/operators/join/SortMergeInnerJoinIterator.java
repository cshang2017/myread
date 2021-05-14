
package org.apache.flink.table.runtime.operators.join;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.runtime.generated.Projection;
import org.apache.flink.table.runtime.generated.RecordComparator;
import org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer;
import org.apache.flink.table.runtime.util.ResettableExternalBuffer;
import org.apache.flink.util.MutableObjectIterator;

import java.io.IOException;

/**
 * Gets probeRow and match rows for inner join.
 */
public class SortMergeInnerJoinIterator extends SortMergeJoinIterator {

	public SortMergeInnerJoinIterator(
			BinaryRowDataSerializer probeSerializer,
			BinaryRowDataSerializer bufferedSerializer,
			Projection probeProjection,
			Projection bufferedProjection,
			RecordComparator keyComparator,
			MutableObjectIterator<RowData> probeIterator,
			MutableObjectIterator<BinaryRowData> bufferedIterator,
			ResettableExternalBuffer buffer,
			boolean[] filterNullKeys) throws IOException {
		super(probeSerializer, bufferedSerializer, probeProjection, bufferedProjection,
				keyComparator, probeIterator, bufferedIterator, buffer, filterNullKeys);
	}

	public boolean nextInnerJoin() throws IOException {
		if (!advanceNextSuitableProbeRow()) {
			return false; // no probe row, over.
		}

		if (matchKey != null && keyComparator.compare(probeKey, matchKey) == 0) {
			return true; // probe has a same key, so same matches.
		}

		if (bufferedRow == null) {
			return false; // no buffered row, bye bye.
		} else {
			// find next equaled key.
			while (true) {
				int cmp = keyComparator.compare(probeKey, bufferedKey);
				if (cmp > 0) {
					if (!advanceNextSuitableBufferedRow()) {
						return false;
					}
				} else if (cmp < 0) {
					if (!advanceNextSuitableProbeRow()) {
						return false;
					}
				} else {
					bufferMatchingRows();
					return true;
				}
			}
		}
	}
}
