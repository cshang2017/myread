package org.apache.flink.runtime.jobmaster;

import java.io.Serializable;

public class SerializedInputSplit implements Serializable {

	private final byte[] inputSplitData;

	public SerializedInputSplit(byte[] inputSplitData) {
		this.inputSplitData = inputSplitData;
	}

	public byte[] getInputSplitData() {
		return inputSplitData;
	}

	public boolean isEmpty() {
		return inputSplitData == null || inputSplitData.length == 0;
	}
}
