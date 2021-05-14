package org.apache.flink.api.java;

import org.apache.flink.annotation.Public;

/**
 * Factory class for execution environments.
 */
@Public
public interface ExecutionEnvironmentFactory {

	/**
	 * Creates an ExecutionEnvironment from this factory.
	 *
	 * @return An ExecutionEnvironment.
	 */
	ExecutionEnvironment createExecutionEnvironment();
}
