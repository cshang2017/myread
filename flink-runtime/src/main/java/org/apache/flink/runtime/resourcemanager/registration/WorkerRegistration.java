package org.apache.flink.runtime.resourcemanager.registration;

import org.apache.flink.runtime.clusterframework.types.ResourceIDRetrievable;
import org.apache.flink.runtime.instance.HardwareDescription;
import org.apache.flink.runtime.taskexecutor.TaskExecutorGateway;
import org.apache.flink.util.Preconditions;

/**
 * This class extends the {@link TaskExecutorConnection}, adding the worker information.
 */
@Getter
public class WorkerRegistration<WorkerType extends ResourceIDRetrievable> extends TaskExecutorConnection {

	private final WorkerType worker;

	private final int dataPort;

	private final HardwareDescription hardwareDescription;

	public WorkerRegistration(
			TaskExecutorGateway taskExecutorGateway,
			WorkerType worker,
			int dataPort,
			HardwareDescription hardwareDescription) {

		super(worker.getResourceID(), taskExecutorGateway);

		this.worker = Preconditions.checkNotNull(worker);
		this.dataPort = dataPort;
		this.hardwareDescription = Preconditions.checkNotNull(hardwareDescription);
	}

}
