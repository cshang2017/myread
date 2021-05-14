package org.apache.flink.api.common.externalresource;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.configuration.Configuration;

/**
 * Factory for {@link ExternalResourceDriver}. Instantiate a driver with configuration.
 *
 * <p>Drivers that can be instantiated with a factory automatically qualify for being loaded as a plugin, so long as
 * the driver jar is self-contained (excluding Flink dependencies) and contains a
 * {@code META-INF/services/org.apache.flink.api.common.externalresource.ExternalResourceDriverFactory} file containing the
 * qualified class name of the factory.
 */
@PublicEvolving
public interface ExternalResourceDriverFactory {
	/**
	 * Construct the ExternalResourceDriver from configuration.
	 *
	 * @param config configuration for this external resource
	 * @return the driver for this external resource
	 * @throws Exception if there is something wrong during the creation
	 */
	ExternalResourceDriver createExternalResourceDriver(Configuration config) throws Exception;
}
