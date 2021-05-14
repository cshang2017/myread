package org.apache.flink.runtime.taskmanager;

import org.apache.flink.runtime.util.FatalExitExceptionHandler;

import java.util.concurrent.ThreadFactory;

/**
 * Thread factory that creates threads with a given name, associates them with a given
 * thread group, and set them to daemon mode.
 */
public class DispatcherThreadFactory implements ThreadFactory {

	private final ThreadGroup group;

	private final String threadName;

	/**
	 * Creates a new thread factory.
	 *
	 * @param group The group that the threads will be associated with.
	 * @param threadName The name for the threads.
	 */
	public DispatcherThreadFactory(ThreadGroup group, String threadName) {
		this.group = group;
		this.threadName = threadName;
	}

	@Override
	public Thread newThread(Runnable r) {
		Thread t = new Thread(group, r, threadName);
		t.setDaemon(true);
		t.setUncaughtExceptionHandler(FatalExitExceptionHandler.INSTANCE);
		return t;
	}
}
