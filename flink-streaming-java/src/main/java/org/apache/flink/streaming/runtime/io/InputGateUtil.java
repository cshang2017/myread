package org.apache.flink.streaming.runtime.io;

import org.apache.flink.annotation.Internal;
import org.apache.flink.runtime.io.network.partition.consumer.IndexedInputGate;
import org.apache.flink.runtime.io.network.partition.consumer.InputGate;
import org.apache.flink.runtime.io.network.partition.consumer.UnionInputGate;

import java.util.List;

/**
 * Utility for dealing with input gates. This will either just return
 * the single {@link InputGate} that was passed in or create a {@link UnionInputGate} if several
 * {@link InputGate input gates} are given.
 */
@Internal
public class InputGateUtil {

	public static InputGate createInputGate(List<IndexedInputGate> inputGates) {
		if (inputGates.size() <= 0) {
			throw new RuntimeException("No such input gate.");
		}

		if (inputGates.size() == 1) {
			return inputGates.get(0);
		} else {
			return new UnionInputGate(inputGates.toArray(new IndexedInputGate[0]));
		}
	}

	/**
	 * Private constructor to prevent instantiation.
	 */
	private InputGateUtil() {
		throw new RuntimeException();
	}
}
