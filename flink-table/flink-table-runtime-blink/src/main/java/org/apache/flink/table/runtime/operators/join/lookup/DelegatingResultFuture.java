package org.apache.flink.table.runtime.operators.join.lookup;

import org.apache.flink.streaming.api.functions.async.ResultFuture;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;

/**
 * Delegates actions of {@link java.util.concurrent.CompletableFuture} to {@link ResultFuture}.
 * This is used as a bridge between {@link org.apache.flink.table.functions.AsyncTableFunction} and
 * {@link org.apache.flink.streaming.api.functions.async.AsyncFunction}.
 */
public class DelegatingResultFuture<OUT> implements BiConsumer<Collection<OUT>, Throwable> {

	private final ResultFuture<OUT> delegatedResultFuture;
	private final CompletableFuture<Collection<OUT>> completableFuture;

	public DelegatingResultFuture(ResultFuture<OUT> delegatedResultFuture) {
		this.delegatedResultFuture = delegatedResultFuture;
		this.completableFuture = new CompletableFuture<>();
		this.completableFuture.whenComplete(this);
	}

	@Override
	public void accept(Collection<OUT> outs, Throwable throwable) {
		if (throwable != null) {
			delegatedResultFuture.completeExceptionally(throwable);
		} else {
			delegatedResultFuture.complete(outs);
		}
	}

	public CompletableFuture<Collection<OUT>> getCompletableFuture() {
		return completableFuture;
	}
}
