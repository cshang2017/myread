package org.apache.flink.runtime.source.coordinator;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.source.ReaderInfo;
import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.connector.source.SplitsAssignment;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.runtime.operators.coordination.TaskNotRunningException;
import org.apache.flink.runtime.source.event.AddSplitEvent;
import org.apache.flink.runtime.source.event.NoMoreSplitsEvent;
import org.apache.flink.runtime.source.event.SourceEventWrapper;
import org.apache.flink.runtime.util.ExecutorThreadFactory;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.ThrowableCatchingRunnable;


import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

import static org.apache.flink.runtime.source.coordinator.SourceCoordinatorSerdeUtils.readRegisteredReaders;
import static org.apache.flink.runtime.source.coordinator.SourceCoordinatorSerdeUtils.writeRegisteredReaders;

/**
 * A context class for the {@link OperatorCoordinator}. Compared with {@link SplitEnumeratorContext} this class
 * allows interaction with state and sending {@link OperatorEvent} to the SourceOperator while
 * {@link SplitEnumeratorContext} only allows sending {@link SourceEvent}.
 *
 * <p>The context serves a few purposes:
 * <ul>
 *     <li>
 *         Information provider - The context provides necessary information to the enumerator for it to
 *         know what is the status of the source readers and their split assignments. These information
 *         allows the split enumerator to do the coordination.
 *     </li>
 *     <li>
 *         Action taker - The context also provides a few actions that the enumerator can take to carry
 *         out the coordination. So far there are two actions: 1) assign splits to the source readers.
 *         and 2) sens a custom {@link SourceEvent SourceEvents} to the source readers.
 *     </li>
 *     <li>
 *         Thread model enforcement - The context ensures that all the manipulations to the coordinator state
 *         are handled by the same thread.
 *     </li>
 * </ul>
 * @param <SplitT> the type of the splits.
 */
@Internal
public class SourceCoordinatorContext<SplitT extends SourceSplit>
		implements SplitEnumeratorContext<SplitT>, AutoCloseable {

	private final ExecutorService coordinatorExecutor;
	private final ExecutorNotifier notifier;
	private final OperatorCoordinator.Context operatorCoordinatorContext;
	private final SimpleVersionedSerializer<SplitT> splitSerializer;
	private final ConcurrentMap<Integer, ReaderInfo> registeredReaders;
	private final SplitAssignmentTracker<SplitT> assignmentTracker;
	private final SourceCoordinatorProvider.CoordinatorExecutorThreadFactory coordinatorThreadFactory;
	private final String coordinatorThreadName;

	public SourceCoordinatorContext(
			ExecutorService coordinatorExecutor,
			SourceCoordinatorProvider.CoordinatorExecutorThreadFactory coordinatorThreadFactory,
			int numWorkerThreads,
			OperatorCoordinator.Context operatorCoordinatorContext,
			SimpleVersionedSerializer<SplitT> splitSerializer) {
		this(coordinatorExecutor, coordinatorThreadFactory, numWorkerThreads, operatorCoordinatorContext,
				splitSerializer, new SplitAssignmentTracker<>());
	}

	// Package private method for unit test.
	SourceCoordinatorContext(
			ExecutorService coordinatorExecutor,
			SourceCoordinatorProvider.CoordinatorExecutorThreadFactory coordinatorThreadFactory,
			int numWorkerThreads,
			OperatorCoordinator.Context operatorCoordinatorContext,
			SimpleVersionedSerializer<SplitT> splitSerializer,
			SplitAssignmentTracker<SplitT> splitAssignmentTracker) {
		this.coordinatorExecutor = coordinatorExecutor;
		this.coordinatorThreadFactory = coordinatorThreadFactory;
		this.operatorCoordinatorContext = operatorCoordinatorContext;
		this.splitSerializer = splitSerializer;
		this.registeredReaders = new ConcurrentHashMap<>();
		this.assignmentTracker = splitAssignmentTracker;
		this.coordinatorThreadName = coordinatorThreadFactory.getCoordinatorThreadName();

		final Executor errorHandlingCoordinatorExecutor = (runnable) ->
				coordinatorExecutor.execute(new ThrowableCatchingRunnable(this::handleUncaughtExceptionFromAsyncCall, runnable));

		this.notifier = new ExecutorNotifier(
				Executors.newScheduledThreadPool(numWorkerThreads, new ExecutorThreadFactory(coordinatorThreadName + "-worker")),
				errorHandlingCoordinatorExecutor);
	}

	@Override
	public MetricGroup metricGroup() {
		return null;
	}

	@Override
	public void sendEventToSourceReader(int subtaskId, SourceEvent event) {
		callInCoordinatorThread(() -> {
				operatorCoordinatorContext.sendEvent(new SourceEventWrapper(event), subtaskId);
				return null;
		}, String.format("Failed to send event %s to subtask %d", event, subtaskId));
	}

	@Override
	public int currentParallelism() {
		return operatorCoordinatorContext.currentParallelism();
	}

	@Override
	public Map<Integer, ReaderInfo> registeredReaders() {
		return Collections.unmodifiableMap(registeredReaders);
	}

	@Override
	public void assignSplits(SplitsAssignment<SplitT> assignment) {
		// Ensure the split assignment is done by the the coordinator executor.
		callInCoordinatorThread(() -> {
			// Ensure all the subtasks in the assignment have registered.
			for (Integer subtaskId : assignment.assignment().keySet()) {
				if (!registeredReaders.containsKey(subtaskId)) {
					throw new IllegalArgumentException(String.format(
							"Cannot assign splits %s to subtask %d because the subtask is not registered.",
							registeredReaders.get(subtaskId), subtaskId));
				}
			}

			assignmentTracker.recordSplitAssignment(assignment);
			assignment.assignment().forEach(
					(id, splits) -> {
							operatorCoordinatorContext.sendEvent(new AddSplitEvent<>(splits, splitSerializer), id);
					});
			return null;
		}, String.format("Failed to assign splits %s due to ", assignment));
	}

	@Override
	public void signalNoMoreSplits(int subtask) {
		// Ensure the split assignment is done by the the coordinator executor.
		callInCoordinatorThread(() -> {
				operatorCoordinatorContext.sendEvent(new NoMoreSplitsEvent(), subtask);
				return null; // void return value
		}, "Failed to send 'NoMoreSplits' to reader " + subtask);
	}

	@Override
	public <T> void callAsync(
			Callable<T> callable,
			BiConsumer<T, Throwable> handler,
			long initialDelay,
			long period) {
		notifier.notifyReadyAsync(callable, handler, initialDelay, period);
	}

	@Override
	public <T> void callAsync(Callable<T> callable, BiConsumer<T, Throwable> handler) {
		notifier.notifyReadyAsync(callable, handler);
	}

	@Override
	public void runInCoordinatorThread(Runnable runnable) {
		coordinatorExecutor.execute(runnable);
	}

	@Override
	public void close() throws InterruptedException {
		notifier.close();
		coordinatorExecutor.shutdown();
		coordinatorExecutor.awaitTermination(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
	}

	// --------- Package private additional methods for the SourceCoordinator ------------

	/**
	 * Fail the job with the given cause.
	 *
	 * @param cause the cause of the job failure.
	 */
	void failJob(Throwable cause) {
		operatorCoordinatorContext.failJob(cause);
	}

	void handleUncaughtExceptionFromAsyncCall(Throwable t) {
		ExceptionUtils.rethrowIfFatalErrorOrOOM(t);
		failJob(t);
	}

	/**
	 * Take a snapshot of this SourceCoordinatorContext.
	 *
	 * @param checkpointId The id of the ongoing checkpoint.
	 * @param splitSerializer The serializer of the splits.
	 * @param out An ObjectOutput that can be used to
	 */
	void snapshotState(
			long checkpointId,
			SimpleVersionedSerializer<SplitT> splitSerializer,
			DataOutputStream out) throws Exception {
		writeRegisteredReaders(registeredReaders, out);
		assignmentTracker.snapshotState(checkpointId, splitSerializer, out);
	}

	/**
	 * Restore the state of the context.
	 * @param splitSerializer the serializer for the SourceSplits.
	 * @param in the input from which the states are read.
	 * @throws Exception when the restoration failed.
	 */
	void restoreState(
			SimpleVersionedSerializer<SplitT> splitSerializer,
			DataInputStream in) throws Exception {
		Map<Integer, ReaderInfo> readers = readRegisteredReaders(in);
		registeredReaders.clear();
		registeredReaders.putAll(readers);
		assignmentTracker.restoreState(splitSerializer, in);
	}

	/**
	 * Register a source reader.
	 *
	 * @param readerInfo the reader information of the source reader.
	 */
	void registerSourceReader(ReaderInfo readerInfo) {
		registeredReaders.put(readerInfo.getSubtaskId(), readerInfo);
	}

	/**
	 * Unregister a source reader.
	 *
	 * @param subtaskId the subtask id of the source reader.
	 */
	void unregisterSourceReader(int subtaskId) {
		registeredReaders.remove(subtaskId);
	}

	/**
	 * Get the split to put back. This only happens when a source reader subtask has failed.
	 *
	 * @param subtaskId the failed subtask id.
	 * @param restoredCheckpointId the checkpoint that the task is recovered to.
	 * @return A list of splits that needs to be added back to the {@link SplitEnumerator}.
	 */
	List<SplitT> getAndRemoveUncheckpointedAssignment(int subtaskId, long restoredCheckpointId) {
		return assignmentTracker.getAndRemoveUncheckpointedAssignment(subtaskId, restoredCheckpointId);
	}

	/**
	 * Invoked when a successful checkpoint has been taken.
	 *
	 * @param checkpointId the id of the successful checkpoint.
	 */
	void onCheckpointComplete(long checkpointId) {
		assignmentTracker.onCheckpointComplete(checkpointId);
	}

	OperatorCoordinator.Context getCoordinatorContext() {
		return operatorCoordinatorContext;
	}

	// ---------------- private helper methods -----------------

	/**
	 * A helper method that delegates the callable to the coordinator thread if the
	 * current thread is not the coordinator thread, otherwise call the callable right away.
	 *
	 * @param callable the callable to delegate.
	 */
	private <V> V callInCoordinatorThread(Callable<V> callable, String errorMessage) {
		// Ensure the split assignment is done by the the coordinator executor.
		if (!coordinatorThreadFactory.isCurrentThreadCoordinatorThread()) {
				return coordinatorExecutor.submit(callable).get();
		}

			return callable.call();
	}
}
