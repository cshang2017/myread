

package org.apache.flink.runtime.taskexecutor.rpc;

import java.io.IOException;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.ClosureCleaner;
import org.apache.flink.runtime.jobmaster.JobMasterGateway;
import org.apache.flink.runtime.taskexecutor.GlobalAggregateManager;
import org.apache.flink.util.InstantiationUtil;

public class RpcGlobalAggregateManager implements GlobalAggregateManager {

	private final JobMasterGateway jobMasterGateway;

	public RpcGlobalAggregateManager(JobMasterGateway jobMasterGateway) {
		this.jobMasterGateway = jobMasterGateway;
	}

	@Override
	public <IN, ACC, OUT> OUT updateGlobalAggregate(String aggregateName, Object aggregand, AggregateFunction<IN, ACC, OUT> aggregateFunction)
		throws IOException {
		ClosureCleaner.clean(aggregateFunction, ExecutionConfig.ClosureCleanerLevel.RECURSIVE,true);
		byte[] serializedAggregateFunction = InstantiationUtil.serializeObject(aggregateFunction);
		Object result = null;
			result = jobMasterGateway.updateGlobalAggregate(aggregateName, aggregand, serializedAggregateFunction).get();
		return (OUT) result;
	}
}

