package org.apache.flink.runtime.operators;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypeSerializerFactory;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.metrics.groups.OperatorMetricGroup;
import org.apache.flink.runtime.operators.util.TaskConfig;
import org.apache.flink.runtime.taskmanager.TaskManagerRuntimeInfo;
import org.apache.flink.util.Collector;
import org.apache.flink.util.MutableObjectIterator;


/**
 * The task context gives a driver (e.g., {@link MapDriver}, or {@link JoinDriver}) access to
 * the runtime components and configuration that they can use to fulfil their task.
 *
 * @param <S> The UDF type.
 * @param <OT> The produced data type.
 *
 * @see Driver
 */
public interface TaskContext<S, OT> {
	
	TaskConfig getTaskConfig();
	
	TaskManagerRuntimeInfo getTaskManagerInfo();

	ClassLoader getUserCodeClassLoader();
	
	MemoryManager getMemoryManager();
	
	IOManager getIOManager();

	<X> MutableObjectIterator<X> getInput(int index);
	
	<X> TypeSerializerFactory<X> getInputSerializer(int index);
	
	<X> TypeComparator<X> getDriverComparator(int index);
	
	S getStub();

	ExecutionConfig getExecutionConfig();

	Collector<OT> getOutputCollector();
	
	AbstractInvokable getContainingTask();
	
	String formatLogString(String message);
	
	OperatorMetricGroup getMetricGroup();
}
