package org.apache.flink.core.plugin;

import org.apache.flink.util.function.FunctionUtils;

import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.stream.Collectors;

/**
 * This class is used to create a collection of {@link PluginDescriptor} based on directory structure for a given plugin
 * root folder.
 *
 * <p>The expected structure is as follows: the given plugins root folder, containing the plugins folder. One plugin folder
 * contains all resources (jar files) belonging to a plugin. The name of the plugin folder becomes the plugin id.
 * <pre>
 * plugins-root-folder/
 *            |------------plugin-a/ (folder of plugin a)
 *            |                |-plugin-a-1.jar (the jars containing the classes of plugin a)
 *            |                |-plugin-a-2.jar
 *            |                |-...
 *            |
 *            |------------plugin-b/
 *            |                |-plugin-b-1.jar
 *           ...               |-...
 * </pre>
 */
public class DirectoryBasedPluginFinder implements PluginFinder {

	/** Pattern to match jar files in a directory. */
	private static final String JAR_MATCHER_PATTERN = "glob:**.jar";

	/** Root directory to the plugin folders. */
	private final Path pluginsRootDir;

	/** Matcher for jar files in the filesystem of the root folder. */
	private final PathMatcher jarFileMatcher;

	public DirectoryBasedPluginFinder(Path pluginsRootDir) {
		this.pluginsRootDir = pluginsRootDir;
		this.jarFileMatcher = pluginsRootDir.getFileSystem().getPathMatcher(JAR_MATCHER_PATTERN);
	}

	@Override
	public Collection<PluginDescriptor> findPlugins() {

		return Files.list(pluginsRootDir)
			.filter((Path path) -> Files.isDirectory(path))
			.map(FunctionUtils.uncheckedFunction(this::createPluginDescriptorForSubDirectory))
			.collect(Collectors.toList());
	}

	private PluginDescriptor createPluginDescriptorForSubDirectory(Path subDirectory) {
		URL[] urls = createJarURLsFromDirectory(subDirectory);
		Arrays.sort(urls, Comparator.comparing(URL::toString));
		return new PluginDescriptor(
				subDirectory.getFileName().toString(),
				urls,
				new String[0]);
	}

	private URL[] createJarURLsFromDirectory(Path subDirectory) throws IOException {
		URL[] urls = Files.list(subDirectory)
			.filter((Path p) -> Files.isRegularFile(p) && jarFileMatcher.matches(p))
			.map(FunctionUtils.uncheckedFunction((Path p) -> p.toUri().toURL()))
			.toArray(URL[]::new);

		return urls;
	}
}
