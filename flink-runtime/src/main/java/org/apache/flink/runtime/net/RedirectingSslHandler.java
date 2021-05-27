package org.apache.flink.runtime.net;

import org.apache.flink.runtime.io.network.netty.SSLHandlerFactory;
import org.apache.flink.runtime.rest.handler.util.HandlerRedirectUtils;
import org.apache.flink.runtime.rest.handler.util.KeepAliveWrite;

import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelFutureListener;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelInboundHandlerAdapter;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.ByteToMessageDecoder;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpRequest;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponse;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpServerCodec;
import org.apache.flink.shaded.netty4.io.netty.handler.ssl.SslHandler;
import org.apache.flink.shaded.netty4.io.netty.util.ReferenceCountUtil;

import javax.annotation.Nonnull;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/** SSL handler which automatically redirects Non-SSL requests to SSL address. */
public class RedirectingSslHandler extends ByteToMessageDecoder {

	private static final String SSL_HANDLER_NAME = "ssl";
	private static final String HTTP_CODEC_HANDLER_NAME = "http-codec";
	private static final String NON_SSL_HANDLER_NAME = "redirecting-non-ssl";

	/** the length of the ssl record header (in bytes). */
	private static final int SSL_RECORD_HEADER_LENGTH = 5;

	@Nonnull private final String confRedirectBaseUrl;
	@Nonnull private final CompletableFuture<String> redirectBaseUrl;
	@Nonnull private final SSLHandlerFactory sslHandlerFactory;

	public RedirectingSslHandler(
		@Nonnull String confRedirectHost,
		@Nonnull CompletableFuture<String> redirectBaseUrl,
		@Nonnull SSLHandlerFactory sslHandlerFactory) {
		this.confRedirectBaseUrl = "https://" + confRedirectHost + ":";
		this.redirectBaseUrl = redirectBaseUrl;
		this.sslHandlerFactory = sslHandlerFactory;
	}

	@Override
	protected void decode(ChannelHandlerContext context, ByteBuf in, List<Object> out) {
		if (in.readableBytes() >= SSL_RECORD_HEADER_LENGTH && SslHandler.isEncrypted(in)) {
			handleSsl(context);
		} else {
			context.pipeline().replace(this, HTTP_CODEC_HANDLER_NAME, new HttpServerCodec());
			context.pipeline().addAfter(HTTP_CODEC_HANDLER_NAME, NON_SSL_HANDLER_NAME, new NonSslHandler());
		}
	}

	private void handleSsl(ChannelHandlerContext context) {
		SslHandler sslHandler = sslHandlerFactory.createNettySSLHandler(context.alloc());
			context.pipeline().replace(this, SSL_HANDLER_NAME, sslHandler);
	}

	private class NonSslHandler extends ChannelInboundHandlerAdapter {
		@Override
		public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
			HttpRequest request = msg instanceof HttpRequest ? (HttpRequest) msg : null;
			String path = request == null ? "" : request.uri();
			String redirectAddress = getRedirectAddress(ctx);
			HttpResponse response = HandlerRedirectUtils.getRedirectResponse(
				redirectAddress, path, HttpResponseStatus.MOVED_PERMANENTLY);
			if (request != null) {
				KeepAliveWrite.flush(ctx, request, response);
			} else {
				ctx.writeAndFlush(response).addListener(ChannelFutureListener.CLOSE);
			}
		}

		private String getRedirectAddress(ChannelHandlerContext ctx) throws Exception {
			return redirectBaseUrl.isDone() ? redirectBaseUrl.get() :
				confRedirectBaseUrl + ((InetSocketAddress) (ctx.channel()).localAddress()).getPort();
		}
	}
}
