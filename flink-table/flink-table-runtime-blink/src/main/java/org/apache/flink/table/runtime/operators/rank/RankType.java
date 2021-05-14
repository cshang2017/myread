

package org.apache.flink.table.runtime.operators.rank;

/**
 * An enumeration of rank type, usable to show how exactly generate rank number.
 */
public enum RankType {

	/**
	 * Returns a unique sequential number for each row within the partition based on the order,
	 * starting at 1 for the first row in each partition and without repeating or skipping
	 * numbers in the ranking result of each partition. If there are duplicate values within the
	 * row set, the ranking numbers will be assigned arbitrarily.
	 */
	ROW_NUMBER,

	/**
	 * Returns a unique rank number for each distinct row within the partition based on the order,
	 * starting at 1 for the first row in each partition, with the same rank for duplicate values
	 * and leaving gaps between the ranks; this gap appears in the sequence after the duplicate
	 * values.
	 */
	RANK,

	/**
	 * is similar to the RANK by generating a unique rank number for each distinct row
	 * within the partition based on the order, starting at 1 for the first row in each partition,
	 * ranking the rows with equal values with the same rank number, except that it does not skip
	 * any rank, leaving no gaps between the ranks.
	 */
	DENSE_RANK
}
