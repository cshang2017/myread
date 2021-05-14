package org.apache.flink.client.cli;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

import java.time.Duration;

/**
 * Describes a client configuration parameter.
 */
@PublicEvolving
public class ClientOptions {

	public static final ConfigOption<Duration> CLIENT_TIMEOUT =
			ConfigOptions.key("client.timeout")
					.durationType()
					.defaultValue(Duration.ofSeconds(60))
					.withDeprecatedKeys("akka.client.timeout") // the deprecated AkkaOptions.CLIENT_TIMEOUT
					.withDescription("Timeout on the client side.");

	public static final ConfigOption<Duration> CLIENT_RETRY_PERIOD =
			ConfigOptions.key("client.retry-period")
					.durationType()
					.defaultValue(Duration.ofMillis(2000))
					.withDescription("The interval (in ms) between consecutive retries of failed attempts to execute " +
							"commands through the CLI or Flink's clients, wherever retry is supported (default 2sec).");
}
