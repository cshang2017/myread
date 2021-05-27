package org.apache.flink.runtime.operators.chaining;

import org.apache.flink.api.common.functions.RichFunction;
import org.apache.flink.api.common.functions.IterationRuntimeContext;
import org.apache.flink.api.common.operators.base.BulkIterationBase;
import org.apache.flink.api.common.operators.base.BulkIterationBase.TerminationCriterionAggregator;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;

public class ChainedTerminationCriterionDriver<IT, OT> extends ChainedDriver<IT, OT> {
	
	private TerminationCriterionAggregator agg;

	// --------------------------------------------------------------------------------------------

	@Override
	public void setup(AbstractInvokable parent) {
		agg = ((IterationRuntimeContext) getUdfRuntimeContext()).getIterationAggregator(BulkIterationBase.TERMINATION_CRITERION_AGGREGATOR_NAME);
	}

	@Override
	public void openTask() {}

	@Override
	public void closeTask() {}

	@Override
	public void cancelTask() {}

	// --------------------------------------------------------------------------------------------

	public RichFunction getStub() {
		return null;
	}

	public String getTaskName() {
		return "";
	}

	// --------------------------------------------------------------------------------------------

	@Override
	public void collect(IT record) {
		numRecordsIn.inc();
		agg.aggregate(1);
	}

	@Override
	public void close() {}
}
