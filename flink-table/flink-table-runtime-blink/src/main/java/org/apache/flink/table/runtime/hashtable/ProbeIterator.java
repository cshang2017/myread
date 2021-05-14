package org.apache.flink.table.runtime.hashtable;

import org.apache.flink.runtime.io.disk.ChannelReaderInputViewIterator;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryRowData;

import java.io.IOException;

/**
 * Probe iterator from probe or spilled partition.
 */
public final class ProbeIterator {

	private ChannelReaderInputViewIterator<BinaryRowData> source;

	private RowData instance;
	private BinaryRowData reuse;

	public ProbeIterator(BinaryRowData instance) {
		this.instance = instance;
	}

	public void set(ChannelReaderInputViewIterator<BinaryRowData> source) {
		this.source = source;
	}

	public void setReuse(BinaryRowData reuse) {
		this.reuse = reuse;
	}

	public BinaryRowData next() throws IOException {
		BinaryRowData retVal = this.source.next(reuse);
		if (retVal != null) {
			this.instance = retVal;
			return retVal;
		} else {
			return null;
		}
	}

	public RowData current() {
		return this.instance;
	}

	public void setInstance(RowData instance) {
		this.instance = instance;
	}

	public boolean hasSource() {
		return source != null;
	}
}
