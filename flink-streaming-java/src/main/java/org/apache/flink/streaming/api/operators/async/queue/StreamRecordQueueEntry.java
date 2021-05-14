package org.apache.flink.streaming.api.operators.async.queue;

import org.apache.flink.annotation.Internal;
import org.apache.flink.streaming.api.functions.async.AsyncFunction;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.operators.TimestampedCollector;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nonnull;

import java.util.Collection;

/**
 * {@link StreamElementQueueEntry} implementation for {@link StreamRecord}. This class also acts
 * as the {@link ResultFuture} implementation which is given to the {@link AsyncFunction}. The
 * async function completes this class with a collection of results.
 *
 * @param <OUT> Type of the asynchronous collection result.
 */
@Internal
class StreamRecordQueueEntry<OUT> implements StreamElementQueueEntry<OUT> {
	@Nonnull
	private final StreamRecord<?> inputRecord;

	private Collection<OUT> completedElements;

	StreamRecordQueueEntry(StreamRecord<?> inputRecord) {
		this.inputRecord = Preconditions.checkNotNull(inputRecord);
	}

	@Override
	public boolean isDone() {
		return completedElements != null;
	}

	@Nonnull
	@Override
	public StreamRecord<?> getInputElement() {
		return inputRecord;
	}

	@Override
	public void emitResult(TimestampedCollector<OUT> output) {
		output.setTimestamp(inputRecord);
		for (OUT r : completedElements) {
			output.collect(r);
		}
	}

	@Override
	public void complete(Collection<OUT> result) {
		this.completedElements = Preconditions.checkNotNull(result);
	}
}
