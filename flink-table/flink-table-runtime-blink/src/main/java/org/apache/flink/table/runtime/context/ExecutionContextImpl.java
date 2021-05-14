package org.apache.flink.table.runtime.context;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Preconditions;


/**
 * Implementation of ExecutionContext.
 */
public final class ExecutionContextImpl implements ExecutionContext {

	private final AbstractStreamOperator<?> operator;
	private final RuntimeContext runtimeContext;

	public ExecutionContextImpl(
			AbstractStreamOperator<?> operator,
			RuntimeContext runtimeContext) {
		this.operator = operator;
		this.runtimeContext = Preconditions.checkNotNull(runtimeContext);
	}

	@Override
	public RowData currentKey() {
		return (RowData) operator.getCurrentKey();
	}

	@Override
	public void setCurrentKey(RowData key) {
		operator.setCurrentKey(key);
	}

	@Override
	public RuntimeContext getRuntimeContext() {
		return runtimeContext;
	}
}
