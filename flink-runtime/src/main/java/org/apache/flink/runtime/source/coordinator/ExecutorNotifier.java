package org.apache.flink.runtime.source.coordinator;

import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;

/**
 * This class is used to coordinate between two components, where one component has an
 * executor following the mailbox model and the other component notifies it when needed.
 */
public class ExecutorNotifier implements AutoCloseable {
	private final ScheduledExecutorService workerExecutor;
	private final Executor executorToNotify;
	private final AtomicBoolean closed;

	public ExecutorNotifier(ScheduledExecutorService workerExecutor,
							Executor executorToNotify) {
		this.executorToNotify = executorToNotify;
		this.workerExecutor = workerExecutor;
		this.closed = new AtomicBoolean(false);
	}

	/**
	 * Call the given callable once. Notify the {@link #executorToNotify} to execute
	 * the handler.
	 *
	 * <p>Note that when this method is invoked multiple times, it is possible that
	 * multiple callables are executed concurrently, so do the handlers. For example,
	 * assuming both the workerExecutor and executorToNotify are single threaded.
	 * The following code may still throw a <code>ConcurrentModificationException</code>.
	 *
	 * <pre>{@code
	 *  final List<Integer> list = new ArrayList<>();
	 *
	 *  // The callable adds an integer 1 to the list, while it works at the first glance,
	 *  // A ConcurrentModificationException may be thrown because the caller and
	 *  // handler may modify the list at the same time.
	 *  notifier.notifyReadyAsync(
	 *  	() -> list.add(1),
	 *  	(ignoredValue, ignoredThrowable) -> list.add(2));
	 * }</pre>
	 *
	 * <p>Instead, the above logic should be implemented in as:
	 * <pre>{@code
	 *  // Modify the state in the handler.
	 *  notifier.notifyReadyAsync(() -> 1, (v, ignoredThrowable) -> {
	 *  	list.add(v));
	 *  	list.add(2);
	 *  });
	 * }</pre>
	 *
	 * @param callable the callable to invoke before notifying the executor.
	 * @param handler the handler to handle the result of the callable.
	 */
	public <T> void notifyReadyAsync(Callable<T> callable, BiConsumer<T, Throwable> handler) {
		workerExecutor.execute(() -> {
				T result = callable.call();
				executorToNotify.execute(() -> handler.accept(result, null));
		});
	}

	/**
	 * Call the given callable once. Notify the {@link #executorToNotify} to execute
	 * the handler.
	 *
	 * <p>Note that when this method is invoked multiple times, it is possible that
	 * multiple callables are executed concurrently, so do the handlers. For example,
	 * assuming both the workerExecutor and executorToNotify are single threaded.
	 * The following code may still throw a <code>ConcurrentModificationException</code>.
	 *
	 * <pre>{@code
	 *  final List<Integer> list = new ArrayList<>();
	 *
	 *  // The callable adds an integer 1 to the list, while it works at the first glance,
	 *  // A ConcurrentModificationException may be thrown because the caller and
	 *  // handler may modify the list at the same time.
	 *  notifier.notifyReadyAsync(
	 *  	() -> list.add(1),
	 *  	(ignoredValue, ignoredThrowable) -> list.add(2));
	 * }</pre>
	 *
	 * <p>Instead, the above logic should be implemented in as:
	 * <pre>{@code
	 *  // Modify the state in the handler.
	 *  notifier.notifyReadyAsync(() -> 1, (v, ignoredThrowable) -> {
	 *  	list.add(v));
	 *  	list.add(2);
	 *  });
	 * }</pre>
	 *
	 * @param callable the callable to execute before notifying the executor to notify.
	 * @param handler the handler that handles the result from the callable.
	 * @param initialDelayMs the initial delay in ms before invoking the given callable.
	 * @param periodMs the interval in ms to invoke the callable.
	 */
	public <T> void notifyReadyAsync(
			Callable<T> callable,
			BiConsumer<T, Throwable> handler,
			long initialDelayMs,
			long periodMs) {
		workerExecutor.scheduleAtFixedRate(() -> {
				T result = callable.call();
				executorToNotify.execute(() -> handler.accept(result, null));
		}, initialDelayMs, periodMs, TimeUnit.MILLISECONDS);
	}

	/**
	 * Close the executor notifier. This is a blocking call which waits for all the
	 * async calls to finish before it returns.
	 *
	 * @throws InterruptedException when interrupted during closure.
	 */
	public void close() throws InterruptedException {
		if (!closed.compareAndSet(false, true)) {
			return;
		}
		// Shutdown the worker executor, so no more worker tasks can run.
		workerExecutor.shutdownNow();
		workerExecutor.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
	}
}
