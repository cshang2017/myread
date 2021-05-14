package org.apache.flink.runtime.jobgraph;

import org.apache.flink.runtime.topology.ResultID;
import org.apache.flink.util.AbstractID;

import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;

import java.util.UUID;

/**
 * Id identifying {@link IntermediateDataSet}.
 */
public class IntermediateDataSetID extends AbstractID implements ResultID {

	/**
	 * Creates an new random intermediate data set ID.
	 */
	public IntermediateDataSetID() {
		super();
	}

	/**
	 * Creates a new intermediate data set ID with the bytes of the given ID.
	 *
	 * @param from The ID to create this ID from.
	 */
	public IntermediateDataSetID(AbstractID from) {
		super(from);
	}

	/**
	 * Creates a new intermediate data set ID with the bytes of the given UUID.
	 *
	 * @param from The UUID to create this ID from.
	 */
	public IntermediateDataSetID(UUID from) {
		super(from.getLeastSignificantBits(), from.getMostSignificantBits());
	}

	private IntermediateDataSetID(long lower, long upper) {
		super(lower, upper);
	}

	public void writeTo(ByteBuf buf) {
		buf.writeLong(lowerPart);
		buf.writeLong(upperPart);
	}

	public static IntermediateDataSetID fromByteBuf(ByteBuf buf) {
		final long lower = buf.readLong();
		final long upper = buf.readLong();
		return new IntermediateDataSetID(lower, upper);
	}
}
