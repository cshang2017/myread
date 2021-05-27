package org.apache.flink.runtime.state;

import javax.annotation.Nullable;
import java.util.concurrent.Future;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.TimeUnit;

/**
 * A {@link Future} that is always done and will just yield the object that was given at creation
 * time.
 *
 * @param <T> The type of object in this {@code Future}.
 */
public class DoneFuture<T> implements RunnableFuture<T> {

	@Nullable
	private final T payload;

	protected DoneFuture(@Nullable T payload) {
		this.payload = payload;
	}

	@Override
	public boolean cancel(boolean mayInterruptIfRunning) {
		return false;
	}

	@Override
	public boolean isCancelled() {
		return false;
	}

	@Override
	public boolean isDone() {
		return true;
	}

	@Override
	public T get() {
		return payload;
	}

	@Override
	public T get(
			long timeout,
			TimeUnit unit) {
		return get();
	}

	@Override
	public void run() {

	}


	public static <T> DoneFuture<T> of(@Nullable T result) {
		return new DoneFuture<>(result);
	}
}
