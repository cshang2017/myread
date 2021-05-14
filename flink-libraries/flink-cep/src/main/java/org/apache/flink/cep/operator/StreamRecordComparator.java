
package org.apache.flink.cep.operator;

import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.io.Serializable;
import java.util.Comparator;

/**
 * Compares two {@link StreamRecord}s based on their timestamp.
 *
 * @param <IN> Type of the value field of the StreamRecord
 */
public class StreamRecordComparator<IN> implements Comparator<StreamRecord<IN>>, Serializable {

	@Override
	public int compare(StreamRecord<IN> o1, StreamRecord<IN> o2) {
		if (o1.getTimestamp() < o2.getTimestamp()) {
			return -1;
		} else if (o1.getTimestamp() > o2.getTimestamp()) {
			return 1;
		} else {
			return 0;
		}
	}
}
