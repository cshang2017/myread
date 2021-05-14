package org.apache.flink.streaming.api.operators;

import org.apache.flink.annotation.Internal;
import org.apache.flink.streaming.api.watermark.Watermark;

import java.io.IOException;

/**
 * A callback registered with the {@link InternalWatermarkCallbackService} service. This callback will
 * be invoked for all keys registered with the service, upon reception of a watermark.
 */
@Internal
public interface OnWatermarkCallback<KEY> {

	/**
	 * The action to be triggered upon reception of a watermark.
	 *
	 * @param key The current key.
	 * @param watermark The current watermark.
	 */
	void onWatermark(KEY key, Watermark watermark) throws IOException;

}
