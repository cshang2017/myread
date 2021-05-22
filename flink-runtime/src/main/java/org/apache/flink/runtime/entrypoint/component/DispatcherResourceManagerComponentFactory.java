
package org.apache.flink.runtime.entrypoint.component;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.blob.BlobServer;
import org.apache.flink.runtime.dispatcher.ArchivedExecutionGraphStore;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.metrics.MetricRegistry;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.webmonitor.retriever.MetricQueryServiceRetriever;

import java.util.concurrent.Executor;

/**
 * Factory for the {@link DispatcherResourceManagerComponent}.
 */
public interface DispatcherResourceManagerComponentFactory {

	DispatcherResourceManagerComponent create(
		Configuration configuration,
		Executor ioExecutor,
		RpcService rpcService,
		HighAvailabilityServices highAvailabilityServices,
		BlobServer blobServer,
		HeartbeatServices heartbeatServices,
		MetricRegistry metricRegistry,
		ArchivedExecutionGraphStore archivedExecutionGraphStore,
		MetricQueryServiceRetriever metricQueryServiceRetriever,
		FatalErrorHandler fatalErrorHandler) throws Exception;
}
