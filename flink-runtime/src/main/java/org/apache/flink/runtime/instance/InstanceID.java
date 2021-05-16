package org.apache.flink.runtime.instance;

import org.apache.flink.util.AbstractID;

/**
 * Class for statistically unique instance IDs.
 */
public class InstanceID extends AbstractID {

	public InstanceID() {
	}

	public InstanceID(byte[] bytes) {
		super(bytes);
	}
}
