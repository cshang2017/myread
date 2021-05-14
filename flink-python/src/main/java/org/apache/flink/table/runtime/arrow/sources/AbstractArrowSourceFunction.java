package org.apache.flink.table.runtime.arrow.sources;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.runtime.TupleSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.runtime.arrow.ArrowReader;
import org.apache.flink.table.runtime.arrow.ArrowUtils;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Preconditions;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorLoader;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ReadChannel;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.channels.Channels;
import java.util.ArrayDeque;
import java.util.Deque;

/**
 * An Arrow {@link SourceFunction} which takes the serialized arrow record batch data as input.
 *
 * @param <OUT> The type of the records produced by this source.
 */
@Internal
public abstract class AbstractArrowSourceFunction<OUT>
		extends RichParallelSourceFunction<OUT>
		implements ResultTypeQueryable<OUT>, CheckpointedFunction {

	static {
		ArrowUtils.checkArrowUsable();
	}

	/**
	 * The type of the records produced by this source.
	 */
	final DataType dataType;

	/**
	 * The array of byte array of the source data. Each element is an array
	 * representing an arrow batch.
	 */
	private final byte[][] arrowData;

	/**
	 * Allocator which is used for byte buffer allocation.
	 */
	private transient BufferAllocator allocator;

	/**
	 * Container that holds a set of vectors for the source data to emit.
	 */
	private transient VectorSchemaRoot root;

	private transient volatile boolean running;

	/**
	 * The indexes of the collection of source data to emit. Each element is a tuple of
	 * the index of the arrow batch and the staring index inside the arrow batch.
	 */
	private transient Deque<Tuple2<Integer, Integer>> indexesToEmit;

	/**
	 * The indexes of the source data which have not been emitted.
	 */
	private transient ListState<Tuple2<Integer, Integer>> checkpointedState;

	AbstractArrowSourceFunction(DataType dataType, byte[][] arrowData) {
		this.dataType = Preconditions.checkNotNull(dataType);
		this.arrowData = Preconditions.checkNotNull(arrowData);
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		allocator = ArrowUtils.getRootAllocator().newChildAllocator("ArrowSourceFunction", 0, Long.MAX_VALUE);
		root = VectorSchemaRoot.create(ArrowUtils.toArrowSchema((RowType) dataType.getLogicalType()), allocator);
		running = true;
	}

	@Override
	public void close() throws Exception {
		try {
			super.close();
		} finally {
				root.close();
				allocator.close();
				
		}
	}

	@Override
	public void initializeState(FunctionInitializationContext context) throws Exception {
		Preconditions.checkState(this.checkpointedState == null,
			"The " + getClass().getSimpleName() + " has already been initialized.");

		this.checkpointedState = context.getOperatorStateStore().getListState(
			new ListStateDescriptor<>(
				"arrow-source-state",
				new TupleSerializer<>(
					(Class<Tuple2<Integer, Integer>>) (Class<?>) Tuple2.class,
					new TypeSerializer[]{IntSerializer.INSTANCE, IntSerializer.INSTANCE})
			)
		);

		this.indexesToEmit = new ArrayDeque<>();
		if (context.isRestored()) {
			// upon restoring
			for (Tuple2<Integer, Integer> v : this.checkpointedState.get()) {
				this.indexesToEmit.add(v);
			}
			LOG.info("Subtask {} restored state: {}.", getRuntimeContext().getIndexOfThisSubtask(), indexesToEmit);
		} else {
			// the first time the job is executed
			final int stepSize = getRuntimeContext().getNumberOfParallelSubtasks();
			final int taskIdx = getRuntimeContext().getIndexOfThisSubtask();

			for (int i = taskIdx; i < arrowData.length; i += stepSize) {
				this.indexesToEmit.add(Tuple2.of(i, 0));
			}
		}
	}

	@Override
	public void snapshotState(FunctionSnapshotContext context) throws Exception {
		Preconditions.checkState(this.checkpointedState != null,
			"The " + getClass().getSimpleName() + " state has not been properly initialized.");

		this.checkpointedState.clear();
		for (Tuple2<Integer, Integer> v : indexesToEmit) {
			this.checkpointedState.add(v);
		}
	}

	@Override
	public void run(SourceContext<OUT> ctx) throws Exception {
		VectorLoader vectorLoader = new VectorLoader(root);
		while (running && !indexesToEmit.isEmpty()) {
			Tuple2<Integer, Integer> indexToEmit = indexesToEmit.peek();
			ArrowRecordBatch arrowRecordBatch = loadBatch(indexToEmit.f0);
			vectorLoader.load(arrowRecordBatch);
			arrowRecordBatch.close();

			ArrowReader<OUT> arrowReader = createArrowReader(root);
			int rowCount = root.getRowCount();
			int nextRowId = indexToEmit.f1;
			while (nextRowId < rowCount) {
				OUT element = arrowReader.read(nextRowId);
				synchronized (ctx.getCheckpointLock()) {
					ctx.collect(element);
					indexToEmit.setField(++nextRowId, 1);
				}
			}

			synchronized (ctx.getCheckpointLock()) {
				indexesToEmit.pop();
			}
		}
	}

	@Override
	public void cancel() {
		running = false;
	}

	abstract ArrowReader<OUT> createArrowReader(VectorSchemaRoot root);

	/**
	 * Load the specified batch of data to process.
	 */
	private ArrowRecordBatch loadBatch(int nextIndexOfArrowDataToProcess) throws IOException {
		ByteArrayInputStream bais = new ByteArrayInputStream(arrowData[nextIndexOfArrowDataToProcess]);
		return MessageSerializer.deserializeRecordBatch(new ReadChannel(Channels.newChannel(bais)), allocator);
	}
}
