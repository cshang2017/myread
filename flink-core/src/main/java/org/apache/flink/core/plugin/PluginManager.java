package org.apache.flink.core.plugin;

import java.util.Iterator;

/**
 * PluginManager is responsible for managing cluster plugins which are loaded using separate class loaders so that their dependencies
 * don't interfere with Flink's dependencies.
 */
public interface PluginManager {

	/**
	 * Returns in iterator over all available implementations of the given service interface (SPI) in all the plugins
	 * known to this plugin manager instance.
	 *
	 * @param service the service interface (SPI) for which implementations are requested.
	 * @param <P> Type of the requested plugin service.
	 * @return Iterator over all implementations of the given service that could be loaded from all known plugins.
	 */
	<P> Iterator<P> load(Class<P> service);
}
