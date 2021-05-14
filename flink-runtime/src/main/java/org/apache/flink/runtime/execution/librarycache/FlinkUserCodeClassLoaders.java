package org.apache.flink.runtime.execution.librarycache;

import org.apache.flink.util.ChildFirstClassLoader;
import org.apache.flink.util.FlinkUserCodeClassLoader;

import java.net.URL;
import java.net.URLClassLoader;
import java.util.function.Consumer;

/**
 * Gives the URLClassLoader a nicer name for debugging purposes.
 */
public class FlinkUserCodeClassLoaders {

	private FlinkUserCodeClassLoaders() {

	}

	public static URLClassLoader parentFirst(
			URL[] urls,
			ClassLoader parent,
			Consumer<Throwable> classLoadingExceptionHandler) {
		return new ParentFirstClassLoader(urls, parent, classLoadingExceptionHandler);
	}

	public static URLClassLoader childFirst(
			URL[] urls,
			ClassLoader parent,
			String[] alwaysParentFirstPatterns,
			Consumer<Throwable> classLoadingExceptionHandler) {
		return new ChildFirstClassLoader(urls, parent, alwaysParentFirstPatterns, classLoadingExceptionHandler);
	}

	public static URLClassLoader create(
			ResolveOrder resolveOrder,
			URL[] urls,
			ClassLoader parent,
			String[] alwaysParentFirstPatterns,
			Consumer<Throwable> classLoadingExceptionHandler) {

		switch (resolveOrder) {
			case CHILD_FIRST:
				return childFirst(urls, parent, alwaysParentFirstPatterns, classLoadingExceptionHandler);
			case PARENT_FIRST:
				return parentFirst(urls, parent, classLoadingExceptionHandler);
			default:
				throw new IllegalArgumentException("Unknown class resolution order: " + resolveOrder);
		}
	}

	/**
	 * Class resolution order for Flink URL {@link ClassLoader}.
	 */
	public enum ResolveOrder {
		CHILD_FIRST, PARENT_FIRST;

		public static ResolveOrder fromString(String resolveOrder) {
			if (resolveOrder.equalsIgnoreCase("parent-first")) {
				return PARENT_FIRST;
			} else if (resolveOrder.equalsIgnoreCase("child-first")) {
				return CHILD_FIRST;
			} else {
				throw new IllegalArgumentException("Unknown resolve order: " + resolveOrder);
			}
		}
	}

	/**
	 * Regular URLClassLoader that first loads from the parent and only after that from the URLs.
	 */
	static class ParentFirstClassLoader extends FlinkUserCodeClassLoader {

		ParentFirstClassLoader(URL[] urls, ClassLoader parent, Consumer<Throwable> classLoadingExceptionHandler) {
			super(urls, parent, classLoadingExceptionHandler);
		}
	}
}
