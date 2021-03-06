package org.apache.flink.table.filesystem.stream;

import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.functions.sink.filesystem.Bucket;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketLifeCycleListener;
import org.apache.flink.streaming.api.functions.sink.filesystem.Buckets;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSinkHelper;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.filesystem.stream.StreamingFileCommitter.CommitMessage;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;

/**
 * Operator for file system sink. It is a operator version of {@link StreamingFileSink}.
 * It sends partition commit message to downstream for committing.
 *
 * <p>See {@link StreamingFileCommitter}.
 */
public class StreamingFileWriter extends AbstractStreamOperator<CommitMessage>
		implements OneInputStreamOperator<RowData, CommitMessage>, BoundedOneInput{

	private final long bucketCheckInterval;

	private final StreamingFileSink.BucketsBuilder<RowData, String, ? extends
			StreamingFileSink.BucketsBuilder<RowData, String, ?>> bucketsBuilder;

	// --------------------------- runtime fields -----------------------------

	private transient Buckets<RowData, String> buckets;

	private transient StreamingFileSinkHelper<RowData> helper;

	private transient long currentWatermark;

	private transient Set<String> currentNewPartitions;
	private transient TreeMap<Long, Set<String>> newPartitions;
	private transient Set<String> committablePartitions;

	public StreamingFileWriter(
			long bucketCheckInterval,
			StreamingFileSink.BucketsBuilder<RowData, String, ? extends
					StreamingFileSink.BucketsBuilder<RowData, String, ?>> bucketsBuilder) {
		this.bucketCheckInterval = bucketCheckInterval;
		this.bucketsBuilder = bucketsBuilder;
		setChainingStrategy(ChainingStrategy.ALWAYS);
	}

	@Override
	public void initializeState(StateInitializationContext context) throws Exception {
		super.initializeState(context);
		buckets = bucketsBuilder.createBuckets(getRuntimeContext().getIndexOfThisSubtask());

		// Set listener before the initialization of Buckets.
		currentNewPartitions = new HashSet<>();
		newPartitions = new TreeMap<>();
		committablePartitions = new HashSet<>();
		buckets.setBucketLifeCycleListener(new BucketLifeCycleListener<RowData, String>() {
			@Override
			public void bucketCreated(Bucket<RowData, String> bucket) {
				currentNewPartitions.add(bucket.getBucketId());
			}

			@Override
			public void bucketInactive(Bucket<RowData, String> bucket) {
				committablePartitions.add(bucket.getBucketId());
			}
		});

		helper = new StreamingFileSinkHelper<>(
				buckets,
				context.isRestored(),
				context.getOperatorStateStore(),
				getRuntimeContext().getProcessingTimeService(),
				bucketCheckInterval);
		currentWatermark = Long.MIN_VALUE;
	}

	@Override
	public void snapshotState(StateSnapshotContext context) throws Exception {
		super.snapshotState(context);
		helper.snapshotState(context.getCheckpointId());
		newPartitions.put(context.getCheckpointId(), new HashSet<>(currentNewPartitions));
		currentNewPartitions.clear();
	}

	@Override
	public void processWatermark(Watermark mark) throws Exception {
		super.processWatermark(mark);
		currentWatermark = mark.getTimestamp();
	}

	@Override
	public void processElement(StreamRecord<RowData> element) throws Exception {
		helper.onElement(
				element.getValue(),
				getProcessingTimeService().getCurrentProcessingTime(),
				element.hasTimestamp() ? element.getTimestamp() : null,
				currentWatermark);
	}

	/**
	 * Commit up to this checkpoint id, also send inactive partitions to downstream for committing.
	 */
	@Override
	public void notifyCheckpointComplete(long checkpointId) throws Exception {
		super.notifyCheckpointComplete(checkpointId);
		commitUpToCheckpoint(checkpointId);
	}

	private void commitUpToCheckpoint(long checkpointId) throws Exception {
		helper.commitUpToCheckpoint(checkpointId);

		NavigableMap<Long, Set<String>> headPartitions = this.newPartitions.headMap(checkpointId, true);
		Set<String> partitions = new HashSet<>(committablePartitions);
		committablePartitions.clear();
		headPartitions.values().forEach(partitions::addAll);
		headPartitions.clear();

		CommitMessage message = new CommitMessage(
				checkpointId,
				getRuntimeContext().getIndexOfThisSubtask(),
				getRuntimeContext().getNumberOfParallelSubtasks(),
				new ArrayList<>(partitions));
		output.collect(new StreamRecord<>(message));
	}

	@Override
	public void endInput() throws Exception {
		buckets.onProcessingTime(Long.MAX_VALUE);
		helper.snapshotState(Long.MAX_VALUE);
		output.emitWatermark(new Watermark(Long.MAX_VALUE));
		commitUpToCheckpoint(Long.MAX_VALUE);
	}

	@Override
	public void dispose() throws Exception {
		super.dispose();
		if (helper != null) {
			helper.close();
		}
	}
}
