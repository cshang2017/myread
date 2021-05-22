package org.apache.flink.runtime.executiongraph;

import org.apache.flink.metrics.Meter;

import java.io.Serializable;

/**
 * An instance of this class represents a snapshot of the io-related metrics of a single task.
 */
public class IOMetrics implements Serializable {

	protected long numRecordsIn;
	protected long numRecordsOut;

	protected long numBytesIn;
	protected long numBytesOut;

	public IOMetrics(Meter recordsIn, Meter recordsOut, Meter bytesIn, Meter bytesOut) {
		this.numRecordsIn = recordsIn.getCount();
		this.numRecordsOut = recordsOut.getCount();
		this.numBytesIn = bytesIn.getCount();
		this.numBytesOut = bytesOut.getCount();
	}

	public IOMetrics(
			long numBytesIn,
			long numBytesOut,
			long numRecordsIn,
			long numRecordsOut) {
		this.numBytesIn = numBytesIn;
		this.numBytesOut = numBytesOut;
		this.numRecordsIn = numRecordsIn;
		this.numRecordsOut = numRecordsOut;
	}

	public long getNumRecordsIn() {
		return numRecordsIn;
	}

	public long getNumRecordsOut() {
		return numRecordsOut;
	}

	public long getNumBytesIn() {
		return numBytesIn;
	}

	public long getNumBytesOut() {
		return numBytesOut;
	}
}
