package org.apache.flink.runtime.operators.chaining;

import org.apache.flink.api.common.functions.Function;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.util.FunctionUtils;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerFactory;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.operators.BatchTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ChainedAllReduceDriver<IT> extends ChainedDriver<IT, IT> {

	private ReduceFunction<IT> reducer;
	private TypeSerializer<IT> serializer;

	private IT base;

	@Override
	public void setup(AbstractInvokable parent) {
		final ReduceFunction<IT> red = BatchTask.instantiateUserCode(this.config, userCodeClassLoader, ReduceFunction.class);
		this.reducer = red;
		FunctionUtils.setFunctionRuntimeContext(red, getUdfRuntimeContext());

		TypeSerializerFactory<IT> serializerFactory = this.config.getInputSerializer(0, userCodeClassLoader);
		this.serializer = serializerFactory.getSerializer();

	}

	@Override
	public void openTask() throws Exception {
		Configuration stubConfig = this.config.getStubParameters();
		BatchTask.openUserCode(this.reducer, stubConfig);
	}

	@Override
	public void closeTask() throws Exception {
		BatchTask.closeUserCode(this.reducer);
	}

	@Override
	public void cancelTask() {
			FunctionUtils.closeFunction(this.reducer);
	}

	// --------------------------------------------------------------------------------------------
	@Override
	public Function getStub() {
		return this.reducer;
	}

	@Override
	public String getTaskName() {
		return this.taskName;
	}

	// --------------------------------------------------------------------------------------------
	@Override
	public void collect(IT record) {
		numRecordsIn.inc();
			if (base == null) {
				base = serializer.copy(record);
			} else {
				base = objectReuseEnabled ? reducer.reduce(base, record) : serializer.copy(reducer.reduce(base, record));
			}
	}

	@Override
	public void close() {
			if (base != null) {
				this.outputCollector.collect(base);
				base = null;
			}
		this.outputCollector.close();
	}
}
