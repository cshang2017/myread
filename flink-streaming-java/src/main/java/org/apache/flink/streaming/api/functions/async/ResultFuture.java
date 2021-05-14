

package org.apache.flink.streaming.api.functions.async;

import org.apache.flink.annotation.PublicEvolving;

import java.util.Collection;

/**
 * {@link ResultFuture} collects data / error in user codes while processing async i/o.
 *
 * @param <OUT> Output type
 */
@PublicEvolving
public interface ResultFuture<OUT> {
	/**
	 * Completes the result future with a collection of result objects.
	 *
	 * <p>Note that it should be called for exactly one time in the user code.
	 * Calling this function for multiple times will cause data lose.
	 *
	 * <p>Put all results in a {@link Collection} and then emit output.
	 *
	 * @param result A list of results.
	 */
	void complete(Collection<OUT> result);

	/**
	 * Completes the result future exceptionally with an exception.
	 *
	 * @param error A Throwable object.
	 */
	void completeExceptionally(Throwable error);
}
