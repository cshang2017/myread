package org.apache.flink.runtime.taskexecutor.rpc;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.tasks.InputSplitProvider;
import org.apache.flink.runtime.jobgraph.tasks.InputSplitProviderException;
import org.apache.flink.runtime.jobmaster.JobMasterGateway;
import org.apache.flink.runtime.jobmaster.SerializedInputSplit;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.Preconditions;

import java.util.concurrent.CompletableFuture;

public class RpcInputSplitProvider implements InputSplitProvider {
	private final JobMasterGateway jobMasterGateway;
	private final JobVertexID jobVertexID;
	private final ExecutionAttemptID executionAttemptID;
	private final Time timeout;

	public RpcInputSplitProvider(
			JobMasterGateway jobMasterGateway,
			JobVertexID jobVertexID,
			ExecutionAttemptID executionAttemptID,
			Time timeout) {
		this.jobMasterGateway = Preconditions.checkNotNull(jobMasterGateway);
		this.jobVertexID = Preconditions.checkNotNull(jobVertexID);
		this.executionAttemptID = Preconditions.checkNotNull(executionAttemptID);
		this.timeout = Preconditions.checkNotNull(timeout);
	}


	@Override
	public InputSplit getNextInputSplit(ClassLoader userCodeClassLoader) throws InputSplitProviderException {
		Preconditions.checkNotNull(userCodeClassLoader);

		CompletableFuture<SerializedInputSplit> futureInputSplit = jobMasterGateway.requestNextInputSplit(
			jobVertexID,
			executionAttemptID);

			SerializedInputSplit serializedInputSplit = futureInputSplit.get(timeout.getSize(), timeout.getUnit());

			if (serializedInputSplit.isEmpty()) {
				return null;
			} else {
				return InstantiationUtil.deserializeObject(serializedInputSplit.getInputSplitData(), userCodeClassLoader);
			}
	}
}
