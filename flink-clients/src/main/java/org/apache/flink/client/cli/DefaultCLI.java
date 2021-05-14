
package org.apache.flink.client.cli;

import org.apache.flink.configuration.Configuration;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;

/**
 * The default CLI which is used for interaction with standalone clusters.
 */
public class DefaultCLI extends AbstractCustomCommandLine {

	public static final String ID = "default";

	public DefaultCLI(Configuration configuration) {
		super(configuration);
	}

	@Override
	public boolean isActive(CommandLine commandLine) {
		// always active because we can try to read a JobManager address from the config
		return true;
	}

	@Override
	public String getId() {
		return ID;
	}

	@Override
	public void addGeneralOptions(Options baseOptions) {
		super.addGeneralOptions(baseOptions);
	}
}
