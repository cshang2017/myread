package org.apache.flink.streaming.api.environment;

import org.apache.flink.annotation.PublicEvolving;

/**
 * Factory class for stream execution environments.
 */
@PublicEvolving
public interface StreamExecutionEnvironmentFactory {

	/**
	 * Creates a StreamExecutionEnvironment from this factory.
	 *
	 * @return A StreamExecutionEnvironment.
	 */
	StreamExecutionEnvironment createExecutionEnvironment();
}
