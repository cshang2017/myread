package org.apache.flink.core.plugin;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.FlinkRuntimeException;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;

/**
 * Utility functions for the plugin mechanism.
 */
public final class PluginUtils {

	private PluginUtils() {
		throw new AssertionError("Singleton class.");
	}

	public static PluginManager createPluginManagerFromRootFolder(Configuration configuration) {
		return createPluginManagerFromRootFolder(PluginConfig.fromConfiguration(configuration));
	}

	private static PluginManager createPluginManagerFromRootFolder(PluginConfig pluginConfig) {
		if (pluginConfig.getPluginsPath().isPresent()) {
				Collection<PluginDescriptor> pluginDescriptors =
					new DirectoryBasedPluginFinder(pluginConfig.getPluginsPath().get()).findPlugins();
				return new DefaultPluginManager(pluginDescriptors, pluginConfig.getAlwaysParentFirstPatterns());
			
		}
		else {
			return new DefaultPluginManager(Collections.emptyList(), pluginConfig.getAlwaysParentFirstPatterns());
		}
	}
}
