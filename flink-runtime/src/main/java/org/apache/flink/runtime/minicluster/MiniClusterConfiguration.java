package org.apache.flink.runtime.minicluster;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.configuration.UnmodifiableConfiguration;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.taskexecutor.TaskExecutorResourceUtils;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import static org.apache.flink.runtime.minicluster.RpcServiceSharing.SHARED;

public class MiniClusterConfiguration {

	private final UnmodifiableConfiguration configuration;
	private final int numTaskManagers;
	private final RpcServiceSharing rpcServiceSharing;

	@Nullable
	private final String commonBindAddress;

	public MiniClusterConfiguration(
			Configuration configuration,
			int numTaskManagers,
			RpcServiceSharing rpcServiceSharing,
			@Nullable String commonBindAddress) {

		this.numTaskManagers = numTaskManagers;
		this.configuration = generateConfiguration(Preconditions.checkNotNull(configuration));
		this.rpcServiceSharing = Preconditions.checkNotNull(rpcServiceSharing);
		this.commonBindAddress = commonBindAddress;
	}

	private UnmodifiableConfiguration generateConfiguration(final Configuration configuration) {
		final Configuration modifiedConfig = new Configuration(configuration);

		TaskExecutorResourceUtils.adjustForLocalExecution(modifiedConfig);

		return new UnmodifiableConfiguration(modifiedConfig);
	}

	public RpcServiceSharing getRpcServiceSharing() {
		return rpcServiceSharing;
	}

	public int getNumTaskManagers() {
		return numTaskManagers;
	}

	public String getJobManagerExternalAddress() {
		return commonBindAddress != null ?
			commonBindAddress :
			configuration.getString(JobManagerOptions.ADDRESS, "localhost");
	}

	public String getTaskManagerExternalAddress() {
		return commonBindAddress != null ?
			commonBindAddress :
			configuration.getString(TaskManagerOptions.HOST, "localhost");
	}

	public String getJobManagerExternalPortRange() {
		return String.valueOf(configuration.getInteger(JobManagerOptions.PORT, 0));
	}

	public String getTaskManagerExternalPortRange() {
		return configuration.getString(TaskManagerOptions.RPC_PORT);
	}

	public String getJobManagerBindAddress() {
		return commonBindAddress != null ?
				commonBindAddress :
				configuration.getString(JobManagerOptions.BIND_HOST, "localhost");
	}

	public String getTaskManagerBindAddress() {
		return commonBindAddress != null ?
				commonBindAddress :
				configuration.getString(TaskManagerOptions.BIND_HOST, "localhost");
	}

	public Time getRpcTimeout() {
		return AkkaUtils.getTimeoutAsTime(configuration);
	}

	public UnmodifiableConfiguration getConfiguration() {
		return configuration;
	}

	/**
	 * Builder for the MiniClusterConfiguration.
	 */
	public static class Builder {
		private Configuration configuration = new Configuration();
		private int numTaskManagers = 1;
		private int numSlotsPerTaskManager = 1;
		private RpcServiceSharing rpcServiceSharing = SHARED;
		@Nullable
		private String commonBindAddress = null;

		public Builder setConfiguration(Configuration configuration1) {
			this.configuration = Preconditions.checkNotNull(configuration1);
			return this;
		}

		public Builder setNumTaskManagers(int numTaskManagers) {
			this.numTaskManagers = numTaskManagers;
			return this;
		}

		public Builder setNumSlotsPerTaskManager(int numSlotsPerTaskManager) {
			this.numSlotsPerTaskManager = numSlotsPerTaskManager;
			return this;
		}

		public Builder setRpcServiceSharing(RpcServiceSharing rpcServiceSharing) {
			this.rpcServiceSharing = Preconditions.checkNotNull(rpcServiceSharing);
			return this;
		}

		public Builder setCommonBindAddress(String commonBindAddress) {
			this.commonBindAddress = commonBindAddress;
			return this;
		}

		public MiniClusterConfiguration build() {
			final Configuration modifiedConfiguration = new Configuration(configuration);
			modifiedConfiguration.setInteger(TaskManagerOptions.NUM_TASK_SLOTS, numSlotsPerTaskManager);
			modifiedConfiguration.setString(
				RestOptions.ADDRESS,
				modifiedConfiguration.getString(RestOptions.ADDRESS, "localhost"));

			return new MiniClusterConfiguration(
				modifiedConfiguration,
				numTaskManagers,
				rpcServiceSharing,
				commonBindAddress);
		}
	}
}
