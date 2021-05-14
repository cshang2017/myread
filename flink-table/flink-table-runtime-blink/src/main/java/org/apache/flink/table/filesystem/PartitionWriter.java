

package org.apache.flink.table.filesystem;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;

import java.io.IOException;

/**
 * Partition writer to write records with partition.
 *
 * <p>See {@link SingleDirectoryWriter}.
 * See {@link DynamicPartitionWriter}.
 * See {@link GroupedPartitionWriter}.
 *
 * @param <T> The type of the consumed records.
 */
@Internal
public interface PartitionWriter<T> {

	/**
	 * Write a record.
	 */
	void write(T in) throws Exception;

	/**
	 * End a transaction.
	 */
	void close() throws Exception;

	/**
	 * Context for partition writer, provide some information and generation utils.
	 */
	class Context<T> {

		private final Configuration conf;
		private final OutputFormatFactory<T> factory;

		public Context(Configuration conf, OutputFormatFactory<T> factory) {
			this.conf = conf;
			this.factory = factory;
		}

		/**
		 * Create a new output format with path, configure it and open it.
		 */
		OutputFormat<T> createNewOutputFormat(Path path) throws IOException {
			OutputFormat<T> format = factory.createOutputFormat(path);
			format.configure(conf);
			// Here we just think of it as a single file format, so there can only be a single task.
			format.open(0, 1);
			return format;
		}
	}
}
