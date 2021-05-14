package org.apache.flink.table.functions.python;

import org.apache.flink.annotation.Internal;
import org.apache.flink.util.Preconditions;

import java.io.Serializable;

/**
 * Python execution environments.
 */
@Internal
public final class PythonEnv implements Serializable {

	/**
	 * The execution type of the Python worker, it defines how to execute the Python functions.
	 */
	private final ExecType execType;

	public PythonEnv(ExecType execType) {
		this.execType = Preconditions.checkNotNull(execType);
	}

	public ExecType getExecType() {
		return execType;
	}

	/**
	 * The Execution type specifies how to execute the Python function.
	 */
	public enum ExecType {
		// python function is executed in a separate process
		PROCESS
	}
}
