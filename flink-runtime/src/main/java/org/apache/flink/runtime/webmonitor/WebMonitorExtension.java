
package org.apache.flink.runtime.webmonitor;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.rest.handler.RestHandlerSpecification;
import org.apache.flink.util.AutoCloseableAsync;

import org.apache.flink.shaded.netty4.io.netty.channel.ChannelInboundHandler;

import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;

/**
 * Interface for {@link WebMonitorEndpoint} extensions. Extensions can register
 * additional handler and allow to close their resources in order to integrate
 * into the life cycle of the {@link WebMonitorEndpoint}.
 */
public interface WebMonitorExtension extends AutoCloseableAsync {

	/**
	 * Gets the collection of extension handlers to register at the {@link WebMonitorEndpoint}.
	 *
	 * @return Collection of handlers to register at the {@link WebMonitorEndpoint}.
	 */
	Collection<Tuple2<RestHandlerSpecification, ChannelInboundHandler>> getHandlers();

	static WebMonitorExtension empty() {
		return EmptyWebMonitorExtension.INSTANCE;
	}

	/**
	 * Empty web monitor extension which can be used as a null object.
	 */
	enum EmptyWebMonitorExtension implements WebMonitorExtension {
		INSTANCE;

		@Override
		public Collection<Tuple2<RestHandlerSpecification, ChannelInboundHandler>> getHandlers() {
			return Collections.emptyList();
		}

		@Override
		public CompletableFuture<Void> closeAsync() {
			return CompletableFuture.completedFuture(null);
		}
	}
}
