package org.apache.flink.core.plugin;

import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;


import java.io.File;
import java.nio.file.Path;
import java.util.Optional;

/**
 * Stores the configuration for plugins mechanism.
 */
@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
public class PluginConfig {

	private final Optional<Path> pluginsPath;

	private final String[] alwaysParentFirstPatterns;

	private PluginConfig(Optional<Path> pluginsPath, String[] alwaysParentFirstPatterns) {
		this.pluginsPath = pluginsPath;
		this.alwaysParentFirstPatterns = alwaysParentFirstPatterns;
	}

	public Optional<Path> getPluginsPath() {
		return pluginsPath;
	}

	public String[] getAlwaysParentFirstPatterns() {
		return alwaysParentFirstPatterns;
	}

	public static PluginConfig fromConfiguration(Configuration configuration) {
		return new PluginConfig(
			getPluginsDir().map(File::toPath),
			CoreOptions.getPluginParentFirstLoaderPatterns(configuration));
	}

	public static Optional<File> getPluginsDir() {
		String pluginsDir = System.getenv().getOrDefault(
			ConfigConstants.ENV_FLINK_PLUGINS_DIR,
			ConfigConstants.DEFAULT_FLINK_PLUGINS_DIRS);

		File pluginsDirFile = new File(pluginsDir);
		if (!pluginsDirFile.isDirectory()) {
			return Optional.empty();
		}
		return Optional.of(pluginsDirFile);
	}
}
