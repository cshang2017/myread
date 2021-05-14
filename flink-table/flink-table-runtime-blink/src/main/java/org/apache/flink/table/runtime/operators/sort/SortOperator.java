package org.apache.flink.table.runtime.operators.sort;

import org.apache.flink.metrics.Gauge;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.runtime.generated.GeneratedNormalizedKeyComputer;
import org.apache.flink.table.runtime.generated.GeneratedRecordComparator;
import org.apache.flink.table.runtime.generated.NormalizedKeyComputer;
import org.apache.flink.table.runtime.generated.RecordComparator;
import org.apache.flink.table.runtime.operators.TableStreamOperator;
import org.apache.flink.table.runtime.typeutils.AbstractRowDataSerializer;
import org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer;
import org.apache.flink.table.runtime.util.StreamRecordCollector;
import org.apache.flink.util.MutableObjectIterator;

/**
 * Operator for batch sort.
 */
public class SortOperator extends TableStreamOperator<BinaryRowData>
		implements OneInputStreamOperator<RowData, BinaryRowData>, BoundedOneInput {


	private GeneratedNormalizedKeyComputer gComputer;
	private GeneratedRecordComparator gComparator;

	private transient BinaryExternalSorter sorter;
	private transient StreamRecordCollector<BinaryRowData> collector;
	private transient BinaryRowDataSerializer binarySerializer;

	public SortOperator(
			GeneratedNormalizedKeyComputer gComputer,
			GeneratedRecordComparator gComparator) {
		this.gComputer = gComputer;
		this.gComparator = gComparator;
	}

	@Override
	public void open() throws Exception {
		super.open();
		LOG.info("Opening SortOperator");

		ClassLoader cl = getContainingTask().getUserCodeClassLoader();

		AbstractRowDataSerializer inputSerializer = (AbstractRowDataSerializer) getOperatorConfig().getTypeSerializerIn1(getUserCodeClassloader());
		this.binarySerializer = new BinaryRowDataSerializer(inputSerializer.getArity());

		NormalizedKeyComputer computer = gComputer.newInstance(cl);
		RecordComparator comparator = gComparator.newInstance(cl);
		gComputer = null;
		gComparator = null;

		MemoryManager memManager = getContainingTask().getEnvironment().getMemoryManager();
		this.sorter = new BinaryExternalSorter(this.getContainingTask(),
				memManager, computeMemorySize(),
				this.getContainingTask().getEnvironment().getIOManager(), inputSerializer,
				binarySerializer, computer, comparator, getContainingTask().getJobConfiguration());
		this.sorter.startThreads();

		collector = new StreamRecordCollector<>(output);

		//register the the metrics.
		getMetricGroup().gauge("memoryUsedSizeInBytes", (Gauge<Long>) sorter::getUsedMemoryInBytes);
		getMetricGroup().gauge("numSpillFiles", (Gauge<Long>) sorter::getNumSpillFiles);
		getMetricGroup().gauge("spillInBytes", (Gauge<Long>) sorter::getSpillInBytes);
	}

	@Override
	public void processElement(StreamRecord<RowData> element) throws Exception {
		this.sorter.write(element.getValue());
	}

	@Override
	public void endInput() throws Exception {
		BinaryRowData row = binarySerializer.createInstance();
		MutableObjectIterator<BinaryRowData> iterator = sorter.getIterator();
		while ((row = iterator.next(row)) != null) {
			collector.collect(row);
		}
	}

	@Override
	public void close() throws Exception {
		LOG.info("Closing SortOperator");
		super.close();
		if (sorter != null) {
			sorter.close();
		}
	}
}
