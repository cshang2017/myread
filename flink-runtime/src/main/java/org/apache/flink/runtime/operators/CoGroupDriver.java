package org.apache.flink.runtime.operators;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.metrics.Counter;
import org.apache.flink.runtime.operators.sort.NonReusingSortMergeCoGroupIterator;
import org.apache.flink.runtime.operators.util.metrics.CountingCollector;
import org.apache.flink.runtime.operators.util.metrics.CountingMutableObjectIterator;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypePairComparatorFactory;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.operators.sort.ReusingSortMergeCoGroupIterator;
import org.apache.flink.runtime.operators.util.CoGroupTaskIterator;
import org.apache.flink.runtime.operators.util.TaskConfig;
import org.apache.flink.util.Collector;
import org.apache.flink.util.MutableObjectIterator;

/**
 * CoGroup task which is executed by a Task Manager. The task has two
 * inputs and one or multiple outputs. It is provided with a CoGroupFunction
 * implementation.
 * <p>
 * The CoGroupTask group all pairs that share the same key from both inputs. Each for each key, the sets of values that
 * were pair with that key of both inputs are handed to the <code>coGroup()</code> method of the CoGroupFunction.
 * 
 * @see org.apache.flink.api.common.functions.CoGroupFunction
 */
public class CoGroupDriver<IT1, IT2, OT> implements Driver<CoGroupFunction<IT1, IT2, OT>, OT> {
	
	
	private TaskContext<CoGroupFunction<IT1, IT2, OT>, OT> taskContext;
	
	private CoGroupTaskIterator<IT1, IT2> coGroupIterator;				// the iterator that does the actual cogroup
	
	private volatile boolean running;

	private boolean objectReuseEnabled = false;

	@Override
	public void setup(TaskContext<CoGroupFunction<IT1, IT2, OT>, OT> context) {
		this.taskContext = context;
		this.running = true;
	}
	

	@Override
	public int getNumberOfInputs() {
		return 2;
	}
	

	@Override
	public Class<CoGroupFunction<IT1, IT2, OT>> getStubType() {
		final Class<CoGroupFunction<IT1, IT2, OT>> clazz 
	    	= (Class<CoGroupFunction<IT1, IT2, OT>>) (Class<?>) CoGroupFunction.class;

		return clazz;
	}
	

	@Override
	public int getNumberOfDriverComparators() {
		return 2;
	}
	

	@Override
	public void prepare() throws Exception
	{
		TaskConfig config = taskContext.getTaskConfig();

		Counter numRecordsIn = taskContext.getMetricGroup().getIOMetricGroup().getNumRecordsInCounter();
		
		MutableObjectIterator<IT1> in1 = new CountingMutableObjectIterator<>(taskContext.<IT1>getInput(0), numRecordsIn);
		MutableObjectIterator<IT2> in2 = new CountingMutableObjectIterator<>(taskContext.<IT2>getInput(1), numRecordsIn);
		
		// get the key positions and types
		TypeSerializer<IT1> serializer1 = taskContext.<IT1>getInputSerializer(0).getSerializer();
		TypeSerializer<IT2> serializer2 = taskContext.<IT2>getInputSerializer(1).getSerializer();
		TypeComparator<IT1> groupComparator1 = taskContext.getDriverComparator(0);
		TypeComparator<IT2> groupComparator2 = taskContext.getDriverComparator(1);
		
		TypePairComparatorFactory<IT1, IT2> pairComparatorFactory = config.getPairComparatorFactory(
					taskContext.getUserCodeClassLoader());

		ExecutionConfig executionConfig = taskContext.getExecutionConfig();
		this.objectReuseEnabled = executionConfig.isObjectReuseEnabled();

		if (objectReuseEnabled) {
			// create CoGroupTaskIterator according to provided local strategy.
			this.coGroupIterator = new ReusingSortMergeCoGroupIterator<IT1, IT2>(
					in1, in2,
					serializer1, groupComparator1,
					serializer2, groupComparator2,
					pairComparatorFactory.createComparator12(groupComparator1, groupComparator2));
		} else {
			// create CoGroupTaskIterator according to provided local strategy.
			this.coGroupIterator = new NonReusingSortMergeCoGroupIterator<IT1, IT2>(
					in1, in2,
					serializer1, groupComparator1,
					serializer2, groupComparator2,
					pairComparatorFactory.createComparator12(groupComparator1, groupComparator2));
		}
		
		// open CoGroupTaskIterator - this triggers the sorting and blocks until the iterator is ready
		this.coGroupIterator.open();
	}
	

	@Override
	public void run() 
	{
		Counter numRecordsOut = this.taskContext.getMetricGroup().getIOMetricGroup().getNumRecordsOutCounter();

		CoGroupFunction<IT1, IT2, OT> coGroupStub = this.taskContext.getStub();
		Collector<OT> collector = new CountingCollector<>(this.taskContext.getOutputCollector(), numRecordsOut);
		CoGroupTaskIterator<IT1, IT2> coGroupIterator = this.coGroupIterator;
		
		while (this.running && coGroupIterator.next()) {
			coGroupStub.coGroup(coGroupIterator.getValues1(), coGroupIterator.getValues2(), collector);
		}
	}


	@Override
	public void cleanup()  {
		if (this.coGroupIterator != null) {
			this.coGroupIterator.close();
			this.coGroupIterator = null;
		}
	}


	@Override
	public void cancel() {
		this.running = false;
		cleanup();
	}
}
