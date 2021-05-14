package org.apache.flink.runtime.jobgraph;

import org.apache.flink.runtime.topology.VertexID;
import org.apache.flink.util.AbstractID;
import org.apache.flink.util.StringUtils;

/**
 * A class for statistically unique job vertex IDs.
 */
public class JobVertexID extends AbstractID implements VertexID {

	private static final long serialVersionUID = 1L;

	public JobVertexID() {
		super();
	}

	public JobVertexID(byte[] bytes) {
		super(bytes);
	}

	public JobVertexID(long lowerPart, long upperPart) {
		super(lowerPart, upperPart);
	}

	public static JobVertexID fromHexString(String hexString) {
		return new JobVertexID(StringUtils.hexStringToByte(hexString));
	}
}
