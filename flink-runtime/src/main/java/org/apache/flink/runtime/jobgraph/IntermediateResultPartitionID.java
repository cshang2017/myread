package org.apache.flink.runtime.jobgraph;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.executiongraph.IntermediateResultPartition;
import org.apache.flink.runtime.topology.ResultID;

import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;

/**
 * Id identifying {@link IntermediateResultPartition}.
 */
public class IntermediateResultPartitionID implements ResultID {

	private final IntermediateDataSetID intermediateDataSetID;
	private final int partitionNum;

	/**
	 * Creates an new random intermediate result partition ID for testing.
	 */
	@VisibleForTesting
	public IntermediateResultPartitionID() {
		this.partitionNum = -1;
		this.intermediateDataSetID = new IntermediateDataSetID();
	}

	/**
	 * Creates an new intermediate result partition ID with {@link IntermediateDataSetID} and the partitionNum.
	 */
	public IntermediateResultPartitionID(IntermediateDataSetID intermediateDataSetID, int partitionNum) {
		this.intermediateDataSetID = intermediateDataSetID;
		this.partitionNum = partitionNum;
	}

	public void writeTo(ByteBuf buf) {
		intermediateDataSetID.writeTo(buf);
		buf.writeInt(partitionNum);
	}

	public static IntermediateResultPartitionID fromByteBuf(ByteBuf buf) {
		final IntermediateDataSetID intermediateDataSetID = IntermediateDataSetID.fromByteBuf(buf);
		final int partitionNum = buf.readInt();
		return new IntermediateResultPartitionID(intermediateDataSetID, partitionNum);
	}

	@Override
	public boolean equals(Object obj) {
		if (obj == this) {
			return true;
		} else if (obj != null && obj.getClass() == getClass()) {
			IntermediateResultPartitionID that = (IntermediateResultPartitionID) obj;
			return that.intermediateDataSetID.equals(this.intermediateDataSetID)
				&& that.partitionNum == this.partitionNum;
		} else {
			return false;
		}
	}

	@Override
	public int hashCode() {
		return this.intermediateDataSetID.hashCode() ^ this.partitionNum;
	}

	@Override
	public String toString() {
		return intermediateDataSetID.toString() + "#" + partitionNum;
	}
}
