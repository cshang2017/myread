package org.apache.flink.table.filesystem;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.filesystem.PartitionWriter.Context;

import java.io.Serializable;
import java.util.LinkedHashMap;

/**
 * Factory of {@link PartitionWriter} to avoid virtual function calls.
 */
@Internal
public interface PartitionWriterFactory<T> extends Serializable {

	PartitionWriter<T> create(
			Context<T> context,
			PartitionTempFileManager manager,
			PartitionComputer<T> computer) throws Exception;

	/**
	 * Util for get a {@link PartitionWriterFactory}.
	 */
	static <T> PartitionWriterFactory<T> get(
			boolean dynamicPartition,
			boolean grouped,
			LinkedHashMap<String, String> staticPartitions) {
		if (dynamicPartition) {
			return grouped ? GroupedPartitionWriter::new : DynamicPartitionWriter::new;
		} else {
			return (PartitionWriterFactory<T>) (context, manager, computer) ->
					new SingleDirectoryWriter<>(context, manager, computer, staticPartitions);
		}
	}
}
