package org.apache.flink.runtime.rest;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.runtime.io.network.netty.SSLHandlerFactory;
import org.apache.flink.runtime.net.SSLUtils;
import org.apache.flink.util.ConfigurationException;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;
import javax.net.ssl.SSLEngine;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * A configuration object for {@link RestClient}s.
 */
public final class RestClientConfiguration {

	@Nullable
	private final SSLHandlerFactory sslHandlerFactory;

	private final long connectionTimeout;

	private final long idlenessTimeout;

	private final int maxContentLength;

	private RestClientConfiguration(
			@Nullable final SSLHandlerFactory sslHandlerFactory,
			final long connectionTimeout,
			final long idlenessTimeout,
			final int maxContentLength) {
		checkArgument(maxContentLength > 0, "maxContentLength must be positive, was: %s", maxContentLength);
		this.sslHandlerFactory = sslHandlerFactory;
		this.connectionTimeout = connectionTimeout;
		this.idlenessTimeout = idlenessTimeout;
		this.maxContentLength = maxContentLength;
	}

	/**
	 * Returns the {@link SSLEngine} that the REST client endpoint should use.
	 *
	 * @return SSLEngine that the REST client endpoint should use, or null if SSL was disabled
	 */
	@Nullable
	public SSLHandlerFactory getSslHandlerFactory() {
		return sslHandlerFactory;
	}

	/**
	 * {@see RestOptions#CONNECTION_TIMEOUT}.
	 */
	public long getConnectionTimeout() {
		return connectionTimeout;
	}

	/**
	 * {@see RestOptions#IDLENESS_TIMEOUT}.
	 */
	public long getIdlenessTimeout() {
		return idlenessTimeout;
	}

	/**
	 * Returns the max content length that the REST client endpoint could handle.
	 *
	 * @return max content length that the REST client endpoint could handle
	 */
	public int getMaxContentLength() {
		return maxContentLength;
	}

	/**
	 * Creates and returns a new {@link RestClientConfiguration} from the given {@link Configuration}.
	 *
	 * @param config configuration from which the REST client endpoint configuration should be created from
	 * @return REST client endpoint configuration
	 * @throws ConfigurationException if SSL was configured incorrectly
	 */

	public static RestClientConfiguration fromConfiguration(Configuration config) throws ConfigurationException {
		Preconditions.checkNotNull(config);

		final SSLHandlerFactory sslHandlerFactory;
		if (SSLUtils.isRestSSLEnabled(config)) {
				sslHandlerFactory = SSLUtils.createRestClientSSLEngineFactory(config);
		} else {
			sslHandlerFactory = null;
		}

		final long connectionTimeout = config.getLong(RestOptions.CONNECTION_TIMEOUT);

		final long idlenessTimeout = config.getLong(RestOptions.IDLENESS_TIMEOUT);

		int maxContentLength = config.getInteger(RestOptions.CLIENT_MAX_CONTENT_LENGTH);

		return new RestClientConfiguration(sslHandlerFactory, connectionTimeout, idlenessTimeout, maxContentLength);
	}
}
