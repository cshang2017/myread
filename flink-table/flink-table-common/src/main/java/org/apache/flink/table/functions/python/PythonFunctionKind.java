package org.apache.flink.table.functions.python;

import org.apache.flink.annotation.Internal;

/**
 * Categorizes the Python functions.
 */
@Internal
public enum PythonFunctionKind {

	GENERAL,

	PANDAS
}
