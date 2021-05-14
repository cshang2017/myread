package org.apache.flink.api.common.serialization;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.core.fs.FSDataOutputStream;

import java.io.IOException;
import java.io.Serializable;

/**
 * An encoder that encodes data in a bulk fashion, encoding many records together at a time.
 *
 * <p>Examples for bulk encoding are most compressed formats, including formats like
 * Parquet and ORC which encode batches of records into blocks of column vectors.
 *
 * <p>The bulk encoder may be stateful and is bound to a single stream during its
 * lifetime.
 *
 * @param <T> The type of the elements encoded through this encoder.
 */
@PublicEvolving
public interface BulkWriter<T> {

	/**
	 * Adds an element to the encoder. The encoder may temporarily buffer the element,
	 * or immediately write it to the stream.
	 *
	 * <p>It may be that adding this element fills up an internal buffer and causes the
	 * encoding and flushing of a batch of internally buffered elements.
	 *
	 * @param element The element to add.
	 * @throws IOException Thrown, if the element cannot be added to the encoder,
	 *                     or if the output stream throws an exception.
	 */
	void addElement(T element) throws IOException;

	/**
	 * Flushes all intermediate buffered data to the output stream.
	 * It is expected that flushing often may reduce the efficiency of the encoding.
	 *
	 * @throws IOException Thrown if the encoder cannot be flushed, or if the output
	 *                     stream throws an exception.
	 */
	void flush() throws IOException;

	/**
	 * Finishes the writing. This must flush all internal buffer, finish encoding, and write
	 * footers.
	 *
	 * <p>The writer is not expected to handle any more records via {@link #addElement(Object)} after
	 * this method is called.
	 *
	 * <p><b>Important:</b> This method MUST NOT close the stream that the writer writes to.
	 * Closing the stream is expected to happen through the invoker of this method afterwards.
	 *
	 * @throws IOException Thrown if the finalization fails.
	 */
	void finish() throws IOException;

	// ------------------------------------------------------------------------

	/**
	 * A factory that creates a {@link BulkWriter}.
	 * @param <T> The type of record to write.
	 */
	@FunctionalInterface
	interface Factory<T> extends Serializable {

		/**
		 * Creates a writer that writes to the given stream.
		 *
		 * @param out The output stream to write the encoded data to.
		 * @throws IOException Thrown if the writer cannot be opened, or if the output
		 *                     stream throws an exception.
		 */
		BulkWriter<T> create(FSDataOutputStream out) throws IOException;
	}
}
