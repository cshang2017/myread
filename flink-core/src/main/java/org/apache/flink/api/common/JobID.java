package org.apache.flink.api.common;

import org.apache.flink.annotation.Public;
import org.apache.flink.util.AbstractID;
import org.apache.flink.util.StringUtils;

import java.nio.ByteBuffer;

/**
 * Unique (at least statistically unique) identifier for a Flink Job. Jobs in Flink correspond
 * to dataflow graphs.
 * 
 * <p>Jobs act simultaneously as <i>sessions</i>, because jobs can be created and submitted
 * incrementally in different parts. Newer fragments of a graph can be attached to existing
 * graphs, thereby extending the current data flow graphs.</p>
 */
@Public
public final class JobID extends AbstractID {

	/**
	 * Creates a new (statistically) random JobID.
	 */
	public JobID() {
		super();
	}

	/**
	 * Creates a new JobID, using the given lower and upper parts.
	 * 
	 * @param lowerPart The lower 8 bytes of the ID. 
	 * @param upperPart The upper 8 bytes of the ID.
	 */
	public JobID(long lowerPart, long upperPart) {
		super(lowerPart, upperPart);
	}

	/**
	 * Creates a new JobID from the given byte sequence. The byte sequence must be
	 * exactly 16 bytes long. The first eight bytes make up the lower part of the ID,
	 * while the next 8 bytes make up the upper part of the ID.
	 *
	 * @param bytes The byte sequence.
	 */
	public JobID(byte[] bytes) {
		super(bytes);
	}
	
	// ------------------------------------------------------------------------
	//  Static factory methods
	// ------------------------------------------------------------------------

	/**
	 * Creates a new (statistically) random JobID.
	 * 
	 * @return A new random JobID.
	 */
	public static JobID generate() {
		return new JobID();
	}

	/**
	 * Creates a new JobID from the given byte sequence. The byte sequence must be
	 * exactly 16 bytes long. The first eight bytes make up the lower part of the ID,
	 * while the next 8 bytes make up the upper part of the ID.
	 * 
	 * @param bytes The byte sequence.
	 *                 
	 * @return A new JobID corresponding to the ID encoded in the bytes.
	 */
	public static JobID fromByteArray(byte[] bytes) {
		return new JobID(bytes);
	}

	public static JobID fromByteBuffer(ByteBuffer buf) {
		long lower = buf.getLong();
		long upper = buf.getLong();
		return new JobID(lower, upper);
	}

	/**
	 * Parses a JobID from the given string.
	 *
	 * @param hexString string representation of a JobID
	 * @return Parsed JobID
	 * @throws IllegalArgumentException if the JobID could not be parsed from the given string
	 */
	public static JobID fromHexString(String hexString) {
			return new JobID(StringUtils.hexStringToByte(hexString));
		
	}
}
