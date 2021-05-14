package org.apache.flink.table.delegation;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.factories.ComponentFactory;

import java.util.Map;

/**
 * Factory that creates {@link Executor}.
 *
 * <p>This factory is used with Java's Service Provider Interfaces (SPI) for discovering. A factory is
 * called with a set of normalized properties that describe the desired configuration. Those properties
 * may include execution configurations such as watermark interval, max parallelism etc., table specific
 * initialization configuration such as if the queries should be executed in batch mode.
 *
 * <p><b>Important:</b> The implementations of this interface should also implement method
 * <pre>{@code public Executor create(Map<String, String> properties, StreamExecutionEnvironment executionEnvironment);}
 * </pre> This method will be used when instantiating a {@link org.apache.flink.table.api.TableEnvironment} from a
 * bridging module which enables conversion from/to {@code DataStream} API and requires a pre configured
 * {@code StreamTableEnvironment}.
 */
@Internal
public interface ExecutorFactory extends ComponentFactory {

	/**
	 * Creates a corresponding {@link Executor}.
	 *
	 * @param properties Static properties of the {@link Executor}, the same that were used for factory lookup.
	 * @return instance of a {@link Executor}
	 */
	Executor create(Map<String, String> properties);
}
