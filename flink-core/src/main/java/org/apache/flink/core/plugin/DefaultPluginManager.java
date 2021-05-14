package org.apache.flink.core.plugin;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;

import org.apache.flink.shaded.guava18.com.google.common.collect.Iterators;

import javax.annotation.concurrent.ThreadSafe;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;

/**
 * Default implementation of {@link PluginManager}.
 */
@Internal
@ThreadSafe
public class DefaultPluginManager implements PluginManager {

	/** Parent-classloader to all classloader that are used for plugin loading. We expect that this is thread-safe. */
	private final ClassLoader parentClassLoader;

	/** A collection of descriptions of all plugins known to this plugin manager. */
	private final Collection<PluginDescriptor> pluginDescriptors;

	/** List of patterns for classes that should always be resolved from the parent ClassLoader. */
	private final String[] alwaysParentFirstPatterns;

	@VisibleForTesting
	DefaultPluginManager() {
		parentClassLoader = null;
		pluginDescriptors = null;
		alwaysParentFirstPatterns = null;
	}

	public DefaultPluginManager(Collection<PluginDescriptor> pluginDescriptors, String[] alwaysParentFirstPatterns) {
		this(pluginDescriptors, DefaultPluginManager.class.getClassLoader(), alwaysParentFirstPatterns);
	}

	public DefaultPluginManager(Collection<PluginDescriptor> pluginDescriptors, ClassLoader parentClassLoader, String[] alwaysParentFirstPatterns) {
		this.pluginDescriptors = pluginDescriptors;
		this.parentClassLoader = parentClassLoader;
		this.alwaysParentFirstPatterns = alwaysParentFirstPatterns;
	}

	@Override
	public <P> Iterator<P> load(Class<P> service) {
		ArrayList<Iterator<P>> combinedIterators = new ArrayList<>(pluginDescriptors.size());
		for (PluginDescriptor pluginDescriptor : pluginDescriptors) {
			PluginLoader pluginLoader = PluginLoader.create(pluginDescriptor, parentClassLoader, alwaysParentFirstPatterns);
			combinedIterators.add(pluginLoader.load(service));
		}
		return Iterators.concat(combinedIterators.iterator());
	}

	@Override
	public String toString() {
		return "PluginManager{" +
			"parentClassLoader=" + parentClassLoader +
			", pluginDescriptors=" + pluginDescriptors +
			", alwaysParentFirstPatterns=" + Arrays.toString(alwaysParentFirstPatterns) +
			'}';
	}
}
