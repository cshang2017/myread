package org.apache.flink.runtime.jobmaster.factories;

import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.io.network.partition.JobMasterPartitionTrackerImpl;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobmanager.OnCompletionActions;
import org.apache.flink.runtime.jobmaster.JobManagerSharedServices;
import org.apache.flink.runtime.jobmaster.JobMaster;
import org.apache.flink.runtime.jobmaster.JobMasterConfiguration;
import org.apache.flink.runtime.scheduler.SchedulerNGFactory;
import org.apache.flink.runtime.jobmaster.slotpool.SchedulerFactory;
import org.apache.flink.runtime.jobmaster.slotpool.SlotPoolFactory;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.shuffle.ShuffleMaster;

/**
 * Default implementation of the {@link JobMasterServiceFactory}.
 */
public class DefaultJobMasterServiceFactory implements JobMasterServiceFactory {

	private final JobMasterConfiguration jobMasterConfiguration;

	private final SlotPoolFactory slotPoolFactory;

	private final SchedulerFactory schedulerFactory;

	private final RpcService rpcService;

	private final HighAvailabilityServices haServices;

	private final JobManagerSharedServices jobManagerSharedServices;

	private final HeartbeatServices heartbeatServices;

	private final JobManagerJobMetricGroupFactory jobManagerJobMetricGroupFactory;

	private final FatalErrorHandler fatalErrorHandler;

	private final SchedulerNGFactory schedulerNGFactory;

	private final ShuffleMaster<?> shuffleMaster;

	public DefaultJobMasterServiceFactory(
			JobMasterConfiguration jobMasterConfiguration,
			SlotPoolFactory slotPoolFactory,
			SchedulerFactory schedulerFactory,
			RpcService rpcService,
			HighAvailabilityServices haServices,
			JobManagerSharedServices jobManagerSharedServices,
			HeartbeatServices heartbeatServices,
			JobManagerJobMetricGroupFactory jobManagerJobMetricGroupFactory,
			FatalErrorHandler fatalErrorHandler,
			SchedulerNGFactory schedulerNGFactory,
			ShuffleMaster<?> shuffleMaster) {
		this.jobMasterConfiguration = jobMasterConfiguration;
		this.slotPoolFactory = slotPoolFactory;
		this.schedulerFactory = schedulerFactory;
		this.rpcService = rpcService;
		this.haServices = haServices;
		this.jobManagerSharedServices = jobManagerSharedServices;
		this.heartbeatServices = heartbeatServices;
		this.jobManagerJobMetricGroupFactory = jobManagerJobMetricGroupFactory;
		this.fatalErrorHandler = fatalErrorHandler;
		this.schedulerNGFactory = schedulerNGFactory;
		this.shuffleMaster = shuffleMaster;
	}

	@Override
	public JobMaster createJobMasterService(
			JobGraph jobGraph,
			OnCompletionActions jobCompletionActions,
			ClassLoader userCodeClassloader) throws Exception {

		return new JobMaster(
			rpcService,
			jobMasterConfiguration,
			ResourceID.generate(),
			jobGraph,
			haServices,
			slotPoolFactory,
			schedulerFactory,
			jobManagerSharedServices,
			heartbeatServices,
			jobManagerJobMetricGroupFactory,
			jobCompletionActions,
			fatalErrorHandler,
			userCodeClassloader,
			schedulerNGFactory,
			shuffleMaster,
			lookup -> new JobMasterPartitionTrackerImpl(
				jobGraph.getJobID(),
				shuffleMaster,
				lookup
			));
	}
}
