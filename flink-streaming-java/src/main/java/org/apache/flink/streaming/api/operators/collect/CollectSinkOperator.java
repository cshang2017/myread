package org.apache.flink.streaming.api.operators.collect;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.runtime.operators.coordination.OperatorEventGateway;
import org.apache.flink.runtime.operators.coordination.OperatorEventHandler;
import org.apache.flink.streaming.api.operators.StreamSink;

import java.util.concurrent.CompletableFuture;

/**
 * A {@link StreamSink} that collects query results and sends them back to the client.
 *
 * @param <IN> type of results to be written into the sink.
 */
public class CollectSinkOperator<IN> extends StreamSink<IN> implements OperatorEventHandler {

	private final CollectSinkFunction<IN> sinkFunction;
	// we need operator id to identify the coordinator of this operator,
	// this is only used for in clients so no need to serialize
	private final transient CompletableFuture<OperatorID> operatorIdFuture;

	public CollectSinkOperator(
			TypeSerializer<IN> serializer,
			int maxResultsPerBatch,
			String accumulatorName) {
		super(new CollectSinkFunction<>(serializer, maxResultsPerBatch, accumulatorName));
		this.sinkFunction = (CollectSinkFunction<IN>) getUserFunction();
		this.operatorIdFuture = new CompletableFuture<>();
	}

	@Override
	public void handleOperatorEvent(OperatorEvent evt) {
		// nothing to handle
	}

	@Override
	public void close() throws Exception {
		sinkFunction.accumulateFinalResults();
		super.close();
	}

	public CompletableFuture<OperatorID> getOperatorIdFuture() {
		return operatorIdFuture;
	}

	void setOperatorEventGateway(OperatorEventGateway operatorEventGateway) {
		sinkFunction.setOperatorEventGateway(operatorEventGateway);
	}
}
