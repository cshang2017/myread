package org.apache.flink.runtime.operators.chaining;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.Function;
import org.apache.flink.api.common.functions.util.FunctionUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.operators.BatchTask;

public class ChainedFlatMapDriver<IT, OT> extends ChainedDriver<IT, OT> {

	private FlatMapFunction<IT, OT> mapper;

	// --------------------------------------------------------------------------------------------

	@Override
	public void setup(AbstractInvokable parent) {
		@SuppressWarnings("unchecked")
		final FlatMapFunction<IT, OT> mapper =
			BatchTask.instantiateUserCode(this.config, userCodeClassLoader, FlatMapFunction.class);
		this.mapper = mapper;
		FunctionUtils.setFunctionRuntimeContext(mapper, getUdfRuntimeContext());
	}

	@Override
	public void openTask() throws Exception {
		Configuration stubConfig = this.config.getStubParameters();
		BatchTask.openUserCode(this.mapper, stubConfig);
	}

	@Override
	public void closeTask() throws Exception {
		BatchTask.closeUserCode(this.mapper);
	}

	@Override
	public void cancelTask() {
		try {
			FunctionUtils.closeFunction(this.mapper);
		}
		catch (Throwable t) {
		}
	}

	// --------------------------------------------------------------------------------------------

	public Function getStub() {
		return this.mapper;
	}

	public String getTaskName() {
		return this.taskName;
	}

	// --------------------------------------------------------------------------------------------

	@Override
	public void collect(IT record) {
		try {
			this.numRecordsIn.inc();
			this.mapper.flatMap(record, this.outputCollector);
		} catch (Exception ex) {
			throw new ExceptionInChainedStubException(this.taskName, ex);
		}
	}

	@Override
	public void close() {
		this.outputCollector.close();
	}

}
