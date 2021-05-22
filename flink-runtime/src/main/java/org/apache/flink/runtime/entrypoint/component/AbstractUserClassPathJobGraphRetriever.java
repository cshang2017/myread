

package org.apache.flink.runtime.entrypoint.component;

import org.apache.flink.util.FileUtils;
import org.apache.flink.util.function.FunctionUtils;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Collections;
import java.util.stream.Collectors;

/**
 *  Abstract class for the JobGraphRetriever which supports getting user classpaths.
 */
public abstract class AbstractUserClassPathJobGraphRetriever implements JobGraphRetriever {

	/** User classpaths in relative form to the working directory. */
	@Nonnull
	private final Collection<URL> userClassPaths;

	protected AbstractUserClassPathJobGraphRetriever(@Nullable File jobDir) throws IOException {
		if (jobDir == null) {
			userClassPaths = Collections.emptyList();
		} else {
			final Path workingDirectory = FileUtils.getCurrentWorkingDirectory();
			final Collection<URL> relativeJarURLs = FileUtils.listFilesInDirectory(jobDir.toPath(), FileUtils::isJarFile)
				.stream()
				.map(path -> FileUtils.relativizePath(workingDirectory, path))
				.map(FunctionUtils.uncheckedFunction(FileUtils::toURL))
				.collect(Collectors.toList());
			this.userClassPaths = Collections.unmodifiableCollection(relativeJarURLs);
		}
	}

	protected Collection<URL> getUserClassPaths() {
		return userClassPaths;
	}
}
