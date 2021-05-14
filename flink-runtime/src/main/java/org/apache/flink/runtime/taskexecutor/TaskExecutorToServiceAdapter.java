package org.apache.flink.runtime.taskexecutor;

import java.util.concurrent.CompletableFuture;

/**
 * Simple adapter for {@link TaskExecutor} to adapt to {@link TaskManagerRunner.TaskExecutorService}.
 */
public class TaskExecutorToServiceAdapter implements TaskManagerRunner.TaskExecutorService {

	private final TaskExecutor taskExecutor;

	private TaskExecutorToServiceAdapter(TaskExecutor taskExecutor) {
		this.taskExecutor = taskExecutor;
	}

	@Override
	public void start() {
		taskExecutor.start();
	}

	@Override
	public CompletableFuture<Void> getTerminationFuture() {
		return taskExecutor.getTerminationFuture();
	}

	@Override
	public CompletableFuture<Void> closeAsync() {
		return taskExecutor.closeAsync();
	}

	public static TaskExecutorToServiceAdapter createFor(TaskExecutor taskExecutor) {
		return new TaskExecutorToServiceAdapter(taskExecutor);
	}
}
