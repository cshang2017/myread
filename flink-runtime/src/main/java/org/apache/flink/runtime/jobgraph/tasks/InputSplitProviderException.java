package org.apache.flink.runtime.jobgraph.tasks;

public class InputSplitProviderException extends Exception {


	public InputSplitProviderException(String message) {
		super(message);
	}

	public InputSplitProviderException(String message, Throwable cause) {
		super(message, cause);
	}

	public InputSplitProviderException(Throwable cause) {
		super(cause);
	}
}
