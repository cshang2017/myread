
package org.apache.flink.runtime.webmonitor.history;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.HistoryServerOptions;
import org.apache.flink.runtime.net.SSLUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.Optional;

/**
 * Utility class for the HistoryServer.
 */
public enum HistoryServerUtils {
	;

	public static boolean isSSLEnabled(Configuration config) {
		return config.getBoolean(HistoryServerOptions.HISTORY_SERVER_WEB_SSL_ENABLED) && SSLUtils.isRestSSLEnabled(config);
	}

	public static Optional<URL> getHistoryServerURL(Configuration configuration) {
		final String hostname = getHostname(configuration);

		if (hostname != null) {
			final String protocol = getProtocol(configuration);
			final int port = getPort(configuration);

				return Optional.of(new URL(protocol, hostname, port, ""));
		} else {
			return Optional.empty();
		}
	}

	private static int getPort(Configuration configuration) {
		return configuration.getInteger(HistoryServerOptions.HISTORY_SERVER_WEB_PORT);
	}

	@Nullable
	private static String getHostname(Configuration configuration) {
		return configuration.getString(HistoryServerOptions.HISTORY_SERVER_WEB_ADDRESS);
	}

	private static String getProtocol(Configuration configuration) {
		if (isSSLEnabled(configuration)) {
			return "https";
		} else {
			return "http";
		}
	}

}
