package org.apache.flink.runtime.util;

/**
 * Utils related to {@link Runnable}.
 */
public class Runnables {

	/**
	 * Guard {@link Runnable} with uncaughtException handler, because
	 * {@link java.util.concurrent.ScheduledExecutorService} does not respect the one assigned to
	 * executing {@link Thread} instance.
	 *
	 * @param runnable Runnable future to guard.
	 * @param uncaughtExceptionHandler Handler to call in case of uncaught exception.
	 * @return Future with handler.
	 */
	public static Runnable withUncaughtExceptionHandler(
		Runnable runnable,
		Thread.UncaughtExceptionHandler uncaughtExceptionHandler) {
		return () -> {
			try {
				runnable.run();
			} catch (Throwable t) {
				uncaughtExceptionHandler.uncaughtException(Thread.currentThread(), t);
			}
		};
	}
}
