package org.apache.flink.table.functions.python;

import org.apache.flink.annotation.Internal;

import java.io.Serializable;

/**
 * The base interface of a wrapper of a Python function. It wraps the serialized Python function
 * and the execution environment.
 */
@Internal
public interface PythonFunction extends Serializable {

	/**
	 * Returns the serialized representation of the user-defined python function.
	 */
	byte[] getSerializedPythonFunction();

	/**
	 * Returns the Python execution environment.
	 */
	PythonEnv getPythonEnv();

	/**
	 * Returns the kind of the user-defined python function.
	 */
	default PythonFunctionKind getPythonFunctionKind() {
		return PythonFunctionKind.GENERAL;
	}
}
