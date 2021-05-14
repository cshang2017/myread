package org.apache.flink.api.connector.source.lib.util;

import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;

import javax.annotation.Nullable;

import java.util.ArrayDeque;
import java.util.Collection;
import java.util.List;
import java.util.Queue;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A {@link SplitEnumerator} for iterator sources. Simply takes the pre-split set of splits and assigns
 * it first-come-first-serve.
 *
 * @param <SplitT> The type of the splits used by the source.
 */
public class IteratorSourceEnumerator<SplitT extends IteratorSourceSplit<?, ?>> implements SplitEnumerator<SplitT, Collection<SplitT>> {

	private final SplitEnumeratorContext<SplitT> context;
	private final Queue<SplitT> remainingSplits;

	public IteratorSourceEnumerator(SplitEnumeratorContext<SplitT> context, Collection<SplitT> splits) {
		this.context = checkNotNull(context);
		this.remainingSplits = new ArrayDeque<>(splits);
	}

	// ------------------------------------------------------------------------

	@Override
	public void start() {}

	@Override
	public void close() {}

	@Override
	public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {
		final SplitT nextSplit = remainingSplits.poll();
		if (nextSplit != null) {
			context.assignSplit(nextSplit, subtaskId);
		} else {
			context.signalNoMoreSplits(subtaskId);
		}
	}

	@Override
	public void addSplitsBack(List<SplitT> splits, int subtaskId) {
		remainingSplits.addAll(splits);
	}

	@Override
	public Collection<SplitT> snapshotState() throws Exception {
		return remainingSplits;
	}

	@Override
	public void addReader(int subtaskId) {
		// we don't assign any splits here, because this registration happens after fist startup
		// and after each reader restart/recovery
		// we only want to assign splits once, initially, which we get by reacting to the readers explicit
		// split request
	}
}
