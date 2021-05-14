package org.apache.flink.api.common.serialization;

import org.apache.flink.annotation.PublicEvolving;

import java.io.IOException;
import java.io.OutputStream;
import java.io.Serializable;

/**
 * A {@link Encoder} is used by the streaming file sink to perform the actual writing
 * of the incoming elements to the files in a bucket.
 *
 * @param <IN> The type of the elements that are being written by the sink.
 */
@PublicEvolving
public interface Encoder<IN> extends Serializable {

	/**
	 * Writes one element to the bucket file.
	 * @param element the element to be written.
	 * @param stream the stream to write the element to.
	 */
	void encode(IN element, OutputStream stream) throws IOException;

}
