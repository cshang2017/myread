package org.apache.flink.streaming.runtime.tasks.mailbox;

import org.apache.flink.annotation.Internal;
import org.apache.flink.streaming.runtime.tasks.StreamTaskActionExecutor;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.function.ThrowingRunnable;

import java.util.concurrent.Future;

/**
 * An executable bound to a specific operator in the chain, such that it can be picked for downstream mailbox.
 */
@Internal
public class Mail {
	/**
	 * The action to execute.
	 */
	private final ThrowingRunnable<? extends Exception> runnable;
	/**
	 * The priority of the mail. The priority does not determine the order, but helps to hide upstream mails from
	 * downstream processors to avoid live/deadlocks.
	 */
	private final int priority;
	/**
	 * The description of the mail that is used for debugging and error-reporting.
	 */
	private final String descriptionFormat;

	private final Object[] descriptionArgs;

	private final StreamTaskActionExecutor actionExecutor;

	public Mail(ThrowingRunnable<? extends Exception> runnable, int priority, String descriptionFormat, Object... descriptionArgs) {
		this(runnable, priority, StreamTaskActionExecutor.IMMEDIATE, descriptionFormat, descriptionArgs);
	}

	public Mail(ThrowingRunnable<? extends Exception> runnable, int priority, StreamTaskActionExecutor actionExecutor, String descriptionFormat, Object... descriptionArgs) {
		this.runnable = Preconditions.checkNotNull(runnable);
		this.priority = priority;
		this.descriptionFormat = descriptionFormat == null ? runnable.toString() : descriptionFormat;
		this.descriptionArgs = Preconditions.checkNotNull(descriptionArgs);
		this.actionExecutor = actionExecutor;
	}

	public int getPriority() {
		return priority;
	}

	public void tryCancel(boolean mayInterruptIfRunning) {
		if (runnable instanceof Future) {
			((Future<?>) runnable).cancel(mayInterruptIfRunning);
		}
	}

	@Override
	public String toString() {
		return String.format(descriptionFormat, descriptionArgs);
	}

	public void run() throws Exception {
		actionExecutor.runThrowing(runnable);
	}
}
