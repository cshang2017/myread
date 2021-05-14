package org.apache.flink.table.functions.python.utils;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.functions.python.PythonFunction;

import java.lang.reflect.InvocationTargetException;

/**
 * Utilities for creating PythonFunction from the fully qualified name of a Python function.
 */
@Internal
public enum PythonFunctionUtils {
	;

	public static PythonFunction getPythonFunction(String fullyQualifiedName, ReadableConfig config) {
			Class pythonFunctionFactory = Class.forName(
				"org.apache.flink.client.python.PythonFunctionFactory",
				true,
				Thread.currentThread().getContextClassLoader());
			return (PythonFunction) pythonFunctionFactory.getMethod(
				"getPythonFunction",
				String.class,
				ReadableConfig.class)
				.invoke(null, fullyQualifiedName, config);
		
	}
}
