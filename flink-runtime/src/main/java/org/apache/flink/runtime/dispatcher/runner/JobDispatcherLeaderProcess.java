package org.apache.flink.runtime.dispatcher.runner;

import org.apache.flink.runtime.dispatcher.DispatcherId;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobmanager.ThrowingJobGraphWriter;
import org.apache.flink.runtime.rpc.FatalErrorHandler;

import java.util.Collections;
import java.util.UUID;

/**
 * {@link DispatcherLeaderProcess} implementation for the per-job mode.
 */
public class JobDispatcherLeaderProcess extends AbstractDispatcherLeaderProcess {

	private final DispatcherGatewayServiceFactory dispatcherGatewayServiceFactory;

	private final JobGraph jobGraph;

	JobDispatcherLeaderProcess(
			UUID leaderSessionId,
			DispatcherGatewayServiceFactory dispatcherGatewayServiceFactory,
			JobGraph jobGraph,
			FatalErrorHandler fatalErrorHandler) {
		super(leaderSessionId, fatalErrorHandler);
		this.jobGraph = jobGraph;
		this.dispatcherGatewayServiceFactory = dispatcherGatewayServiceFactory;
	}

	@Override
	protected void onStart() {
		final DispatcherGatewayService dispatcherService = dispatcherGatewayServiceFactory.create(
			DispatcherId.fromUuid(getLeaderSessionId()),
			Collections.singleton(jobGraph),
			ThrowingJobGraphWriter.INSTANCE);

		completeDispatcherSetup(dispatcherService);
	}
}
