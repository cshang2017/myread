package org.apache.flink.runtime.jobmaster;

import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.registration.RegistrationResponse;
import org.apache.flink.runtime.taskexecutor.TaskExecutor;
import org.apache.flink.util.Preconditions;

/**
 * Message indicating a successful {@link JobMaster} and {@link TaskExecutor} registration.
 */
public class JMTMRegistrationSuccess extends RegistrationResponse.Success {

	private final ResourceID resourceID;

	public JMTMRegistrationSuccess(ResourceID resourceID) {
		this.resourceID = Preconditions.checkNotNull(resourceID);
	}

	public ResourceID getResourceID() {
		return resourceID;
	}
}
