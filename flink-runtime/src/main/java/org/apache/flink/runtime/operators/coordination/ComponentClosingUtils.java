package org.apache.flink.runtime.operators.coordination;

import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.util.function.ThrowingRunnable;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * A util class to help with a clean component shutdown.
 */
public class ComponentClosingUtils {

	/** Utility class, not meant to be instantiated. */
	private ComponentClosingUtils() {}

	/**
	 * Close a component with a timeout.
	 *
	 * @param componentName the name of the component.
	 * @param closingSequence the closing logic which is a callable that can throw exceptions.
	 * @param closeTimeout the timeout to wait for the component to close.
	 * @return An optional throwable which is non-empty if an error occurred when closing the component.
	 */
	public static CompletableFuture<Void> closeAsyncWithTimeout(
			String componentName,
			Runnable closingSequence,
			Duration closeTimeout) {
		return closeAsyncWithTimeout(
				componentName,
				(ThrowingRunnable<Exception>) closingSequence::run,
				closeTimeout);
	}

	/**
	 * Close a component with a timeout.
	 *
	 * @param componentName the name of the component.
	 * @param closingSequence the closing logic.
	 * @param closeTimeout the timeout to wait for the component to close.
	 * @return An optional throwable which is non-empty if an error occurred when closing the component.
	 */
	public static CompletableFuture<Void> closeAsyncWithTimeout(
			String componentName,
			ThrowingRunnable<Exception> closingSequence,
			Duration closeTimeout) {

		final CompletableFuture<Void> future = new CompletableFuture<>();
		// Start a dedicate thread to close the component.
		final Thread t = new Thread(() -> {
				closingSequence.run();
				future.complete(null);
			
		});
		t.start();

		// if the future fails due to a timeout, we interrupt the thread
		future.exceptionally((error) -> {
			if (error instanceof TimeoutException && t.isAlive()) {
				abortThread(t);
			}
			return null;
		});

		FutureUtils.orTimeout(
				future,
				closeTimeout.toMillis(), TimeUnit.MILLISECONDS);

		return future;
	}

	static void abortThread(Thread t) {
		// the abortion strategy is pretty simple here...
		t.interrupt();
	}
}
