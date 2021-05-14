package org.apache.flink.table.runtime.operators;

import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.ChainingStrategy;

/**
 * Table operator to invoke close always.
 */
public class TableStreamOperator<OUT> extends AbstractStreamOperator<OUT> {

	private volatile boolean closed = false;

	public TableStreamOperator() {
		setChainingStrategy(ChainingStrategy.ALWAYS);
	}

	@Override
	public void close()  {
		super.close();
		closed = true;
	}

	@Override
	public void dispose()  {
		if (!closed) {
			close();
		}
		super.dispose();
	}

	public long computeMemorySize() {
		return getContainingTask()
				.getEnvironment()
				.getMemoryManager()
				.computeMemorySize(getOperatorConfig().getManagedMemoryFraction());
	}
}
