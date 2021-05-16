package org.apache.flink.runtime.instance;

import org.apache.flink.util.AbstractID;

public class SlotSharingGroupId extends AbstractID {

	public SlotSharingGroupId(long lowerPart, long upperPart) {
		super(lowerPart, upperPart);
	}

	public SlotSharingGroupId() {
	}
}
