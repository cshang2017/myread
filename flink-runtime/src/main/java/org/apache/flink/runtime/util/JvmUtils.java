package org.apache.flink.runtime.util;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.util.Arrays;
import java.util.Collection;

/**
 * Utilities for {@link java.lang.management.ManagementFactory}.
 */
public final class JvmUtils {

	/**
	 * Creates a thread dump of the current JVM.
	 *
	 * @return the thread dump of current JVM
	 */
	public static Collection<ThreadInfo> createThreadDump() {
		ThreadMXBean threadMxBean = ManagementFactory.getThreadMXBean();

		return Arrays.asList(threadMxBean.dumpAllThreads(true, true));
	}

	/**
	 * Private default constructor to avoid instantiation.
	 */
	private JvmUtils() {}

}
