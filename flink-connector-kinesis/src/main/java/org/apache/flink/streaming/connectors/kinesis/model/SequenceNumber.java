package org.apache.flink.streaming.connectors.kinesis.model;

import org.apache.flink.annotation.Internal;

import java.io.Serializable;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A serializable representation of a Kinesis record's sequence number. It has two fields: the main sequence number,
 * and also a subsequence number. If this {@link SequenceNumber} is referring to an aggregated Kinesis record, the
 * subsequence number will be a non-negative value representing the order of the sub-record within the aggregation.
 */
@Internal
public class SequenceNumber implements Serializable {

	private static final String DELIMITER = "-";

	private final String sequenceNumber;
	private final long subSequenceNumber;

	private final int cachedHash;

	/**
	 * Create a new instance for a non-aggregated Kinesis record without a subsequence number.
	 * @param sequenceNumber the sequence number
	 */
	public SequenceNumber(String sequenceNumber) {
		this(sequenceNumber, -1);
	}

	/**
	 * Create a new instance, with the specified sequence number and subsequence number.
	 * To represent the sequence number for a non-aggregated Kinesis record, the subsequence number should be -1.
	 * Otherwise, give a non-negative sequence number to represent an aggregated Kinesis record.
	 *
	 * @param sequenceNumber the sequence number
	 * @param subSequenceNumber the subsequence number (-1 to represent non-aggregated Kinesis records)
	 */
	public SequenceNumber(String sequenceNumber, long subSequenceNumber) {
		this.sequenceNumber = checkNotNull(sequenceNumber);
		this.subSequenceNumber = subSequenceNumber;

		this.cachedHash = 37 * (sequenceNumber.hashCode() + Long.valueOf(subSequenceNumber).hashCode());
	}

	public boolean isAggregated() {
		return subSequenceNumber >= 0;
	}

	public String getSequenceNumber() {
		return sequenceNumber;
	}

	public long getSubSequenceNumber() {
		return subSequenceNumber;
	}

	@Override
	public String toString() {
		if (isAggregated()) {
			return sequenceNumber + DELIMITER + subSequenceNumber;
		} else {
			return sequenceNumber;
		}
	}

}
