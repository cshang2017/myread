package org.apache.flink.runtime.rest;

import org.apache.flink.configuration.RestOptions;
import org.apache.flink.runtime.rest.handler.util.HandlerUtils;
import org.apache.flink.runtime.rest.messages.ErrorResponseBody;

import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.TooLongFrameException;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpObject;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

import javax.annotation.Nonnull;

import java.util.List;
import java.util.Map;

/**
 * Same as {@link org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpObjectDecoder}
 * but returns HTTP 413 to the client if the payload exceeds {@link #maxContentLength}.
 */
public class FlinkHttpObjectAggregator extends org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpObjectAggregator {

	private final Map<String, String> responseHeaders;

	public FlinkHttpObjectAggregator(final int maxContentLength, @Nonnull final Map<String, String> responseHeaders) {
		super(maxContentLength);
		this.responseHeaders = responseHeaders;
	}

	@Override
	protected void decode(
			final ChannelHandlerContext ctx,
			final HttpObject msg,
			final List<Object> out) throws Exception {

			super.decode(ctx, msg, out);
		
	}
}
