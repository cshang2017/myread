
package org.apache.flink.runtime.operators;

import org.apache.flink.api.common.functions.Function;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.operators.chaining.ChainedDriver;
import org.apache.flink.runtime.operators.chaining.ExceptionInChainedStubException;

/**
 * A chained driver that just passes on the input as the output
 * @param <IT> The type of the input
 */
public class NoOpChainedDriver<IT> extends ChainedDriver<IT, IT> {

	@Override
	public void setup(AbstractInvokable parent) {

	}

	@Override
	public void openTask() throws Exception {

	}

	@Override
	public void closeTask() throws Exception {

	}

	@Override
	public void cancelTask() {

	}

	@Override
	public Function getStub() {
		return null;
	}

	@Override
	public String getTaskName() {
		return this.taskName;
	}

	@Override
	public void collect(IT record) {
			this.numRecordsIn.inc();
			this.outputCollector.collect(record);
	}

	@Override
	public void close() {
		this.outputCollector.close();
	}
}
