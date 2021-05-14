package org.apache.flink.client.cli;

import org.apache.flink.annotation.Internal;
import org.apache.flink.client.deployment.application.ApplicationConfiguration;
import org.apache.flink.configuration.Configuration;

/**
 * An interface to be used by the {@link CliFrontend}
 * to submit user programs for execution.
 */
@Internal
public interface ApplicationDeployer {

	/**
	 * Submits a user program for execution and runs the main user method on the cluster.
	 *
	 * @param configuration the configuration containing all the necessary
	 *                        information about submitting the user program.
	 * @param applicationConfiguration an {@link ApplicationConfiguration} specific to
	 *                                   the application to be executed.
	 */
	<ClusterID> void run(
			final Configuration configuration,
			final ApplicationConfiguration applicationConfiguration) throws Exception;
}
