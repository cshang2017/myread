package org.apache.flink.table.functions.python;

import org.apache.flink.annotation.Internal;
import org.apache.flink.util.Preconditions;

import java.io.Serializable;

/**
 * PythonFunctionInfo contains the execution information of a Python function, such as:
 * the actual Python function, the input arguments, etc.
 */
@Internal
public final class PythonFunctionInfo implements Serializable {

	/**
	 * The python function to be executed.
	 */
	private final PythonFunction pythonFunction;

	/**
	 * The input arguments, it could be an input offset of the input row or
	 * the execution result of another python function described as PythonFunctionInfo.
	 */
	private final Object[] inputs;

	public PythonFunctionInfo(
		PythonFunction pythonFunction,
		Object[] inputs) {
		this.pythonFunction = Preconditions.checkNotNull(pythonFunction);
		this.inputs = Preconditions.checkNotNull(inputs);
	}

	public PythonFunction getPythonFunction() {
		return this.pythonFunction;
	}

	public Object[] getInputs() {
		return this.inputs;
	}
}
