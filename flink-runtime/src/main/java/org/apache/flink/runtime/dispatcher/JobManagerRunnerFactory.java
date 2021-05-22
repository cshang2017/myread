package org.apache.flink.runtime.dispatcher;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobmaster.JobManagerRunner;
import org.apache.flink.runtime.jobmaster.JobManagerSharedServices;
import org.apache.flink.runtime.jobmaster.factories.JobManagerJobMetricGroupFactory;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.RpcService;

/**
 * Factory for a {@link JobManagerRunner}.
 */
@FunctionalInterface
public interface JobManagerRunnerFactory {

	JobManagerRunner createJobManagerRunner(
		JobGraph jobGraph,
		Configuration configuration,
		RpcService rpcService,
		HighAvailabilityServices highAvailabilityServices,
		HeartbeatServices heartbeatServices,
		JobManagerSharedServices jobManagerServices,
		JobManagerJobMetricGroupFactory jobManagerJobMetricGroupFactory,
		FatalErrorHandler fatalErrorHandler) throws Exception;
}
