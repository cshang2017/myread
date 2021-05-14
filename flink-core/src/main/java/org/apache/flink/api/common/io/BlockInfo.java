
package org.apache.flink.api.common.io;

import java.io.IOException;

import org.apache.flink.annotation.Public;
import org.apache.flink.core.io.IOReadableWritable;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

/**
 * A block of 24 bytes written at the <i>end</i> of a block in a binary file, and containing
 * i) the number of records in the block, ii) the accumulated number of records, and
 * iii) the offset of the first record in the block.
 * */
@Public
public class BlockInfo implements IOReadableWritable {

	private long recordCount;

	private long accumulatedRecordCount;

	private long firstRecordStart;

	public int getInfoSize() {
		return 8 + 8 + 8;
	}

	/**
	 * Returns the firstRecordStart.
	 * 
	 * @return the firstRecordStart
	 */
	public long getFirstRecordStart() {
		return this.firstRecordStart;
	}

	/**
	 * Sets the firstRecordStart to the specified value.
	 * 
	 * @param firstRecordStart
	 *        the firstRecordStart to set
	 */
	public void setFirstRecordStart(long firstRecordStart) {
		this.firstRecordStart = firstRecordStart;
	}

	@Override
	public void write(DataOutputView out) throws IOException {
		out.writeLong(this.recordCount);
		out.writeLong(this.accumulatedRecordCount);
		out.writeLong(this.firstRecordStart);
	}

	@Override
	public void read(DataInputView in) throws IOException {
		this.recordCount = in.readLong();
		this.accumulatedRecordCount = in.readLong();
		this.firstRecordStart = in.readLong();
	}

	/**
	 * Returns the recordCount.
	 * 
	 * @return the recordCount
	 */
	public long getRecordCount() {
		return this.recordCount;
	}

	/**
	 * Returns the accumulated record count.
	 * 
	 * @return the accumulated record count
	 */
	public long getAccumulatedRecordCount() {
		return this.accumulatedRecordCount;
	}

	/**
	 * Sets the accumulatedRecordCount to the specified value.
	 * 
	 * @param accumulatedRecordCount
	 *        the accumulatedRecordCount to set
	 */
	public void setAccumulatedRecordCount(long accumulatedRecordCount) {
		this.accumulatedRecordCount = accumulatedRecordCount;
	}

	/**
	 * Sets the recordCount to the specified value.
	 * 
	 * @param recordCount
	 *        the recordCount to set
	 */
	public void setRecordCount(long recordCount) {
		this.recordCount = recordCount;
	}
}
