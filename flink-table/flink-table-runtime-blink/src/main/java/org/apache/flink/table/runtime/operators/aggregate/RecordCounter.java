package org.apache.flink.table.runtime.operators.aggregate;

import org.apache.flink.table.data.RowData;

import java.io.Serializable;

/**
 * The {@link RecordCounter} is used to count the number of input records under the current key.
 */
public abstract class RecordCounter implements Serializable {

	/**
	 * We store the counter in the accumulator. If the counter is not zero, which means
	 * we aggregated at least one record for current key.
	 *
	 * @return true if input record count is zero, false if not.
	 */
	abstract boolean recordCountIsZero(RowData acc);

	/**
	 * Creates a {@link RecordCounter} depends on the index of count(*).
	 * If index is less than zero, returns {@link AccumulationRecordCounter},
	 * otherwise, {@link RetractionRecordCounter}.
	 * @param indexOfCountStar The index of COUNT(*) in the aggregates.
	 *                         -1 when the input doesn't contain COUNT(*), i.e. doesn't contain retraction messages.
	 *                         We make sure there is a COUNT(*) if input stream contains retraction.
	 */
	public static RecordCounter of(int indexOfCountStar) {
		if (indexOfCountStar >= 0) {
			return new RetractionRecordCounter(indexOfCountStar);
		} else {
			return new AccumulationRecordCounter();
		}
	}

	/**
	 * {@link RecordCounter.AccumulationRecordCounter} is a {@link RecordCounter} whose input stream
	 * is append only.
	 */
	private static final class AccumulationRecordCounter extends RecordCounter {

		private static final long serialVersionUID = -7035867949179573822L;

		@Override
		public boolean recordCountIsZero(RowData acc) {
			// when all the inputs are accumulations, the count will never be zero
			return acc == null;
		}
	}

	/**
	 * {@link RecordCounter.RetractionRecordCounter} is a {@link RecordCounter} whose input stream
	 * contains retraction.
	 */
	private static final class RetractionRecordCounter extends RecordCounter {

		private static final long serialVersionUID = 6671010224686975916L;

		private final int indexOfCountStar;

		public RetractionRecordCounter(int indexOfCountStar) {
			this.indexOfCountStar = indexOfCountStar;
		}

		@Override
		public boolean recordCountIsZero(RowData acc) {
			// We store the counter in the accumulator and the counter is never be null
			return acc == null || acc.getLong(indexOfCountStar) == 0;
		}
	}
}
