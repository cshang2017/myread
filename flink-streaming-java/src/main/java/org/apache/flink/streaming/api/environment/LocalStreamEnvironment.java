package org.apache.flink.streaming.api.environment;

import org.apache.flink.annotation.Public;
import org.apache.flink.api.common.InvalidProgramException;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.streaming.api.graph.StreamGraph;

import javax.annotation.Nonnull;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The LocalStreamEnvironment is a StreamExecutionEnvironment that runs the program locally,
 * multi-threaded, in the JVM where the environment is instantiated. It spawns an embedded
 * Flink cluster in the background and executes the program on that cluster.
 *
 * <p>When this environment is instantiated, it uses a default parallelism of {@code 1}. The default
 * parallelism can be set via {@link #setParallelism(int)}.
 */
@Public
public class LocalStreamEnvironment extends StreamExecutionEnvironment {

	/**
	 * Creates a new mini cluster stream environment that uses the default configuration.
	 */
	public LocalStreamEnvironment() {
		this(new Configuration());
	}

	/**
	 * Creates a new mini cluster stream environment that configures its local executor with the given configuration.
	 *
	 * @param configuration The configuration used to configure the local executor.
	 */
	public LocalStreamEnvironment(@Nonnull Configuration configuration) {
		super(validateAndGetConfiguration(configuration));
		setParallelism(1);
	}

	private static Configuration validateAndGetConfiguration(final Configuration configuration) {
		assert(ExecutionEnvironment.areExplicitEnvironmentsAllowed());
		final Configuration effectiveConfiguration = new Configuration(checkNotNull(configuration));
		effectiveConfiguration.set(DeploymentOptions.TARGET, "local");
		effectiveConfiguration.set(DeploymentOptions.ATTACHED, true);
		return effectiveConfiguration;
	}

	@Override
	public JobExecutionResult execute(StreamGraph streamGraph) throws Exception {
		return super.execute(streamGraph);
	}
}
