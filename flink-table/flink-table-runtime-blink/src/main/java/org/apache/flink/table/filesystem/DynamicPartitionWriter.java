package org.apache.flink.table.filesystem;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.io.OutputFormat;

import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.table.utils.PartitionPathUtils.generatePartitionPath;

/**
 * Dynamic partition writer to writing multiple partitions at the same time, it maybe consumes more memory.
 */
@Internal
public class DynamicPartitionWriter<T> implements PartitionWriter<T> {

	private final Context<T> context;
	private final PartitionTempFileManager manager;
	private final PartitionComputer<T> computer;
	private final Map<String, OutputFormat<T>> formats;

	public DynamicPartitionWriter(
			Context<T> context,
			PartitionTempFileManager manager,
			PartitionComputer<T> computer) {
		this.context = context;
		this.manager = manager;
		this.computer = computer;
		this.formats = new HashMap<>();
	}

	@Override
	public void write(T in) throws Exception {
		String partition = generatePartitionPath(computer.generatePartValues(in));
		OutputFormat<T> format = formats.get(partition);

		if (format == null) {
			// create a new format to write new partition.
			format = context.createNewOutputFormat(manager.createPartitionDir(partition));
			formats.put(partition, format);
		}
		format.writeRecord(computer.projectColumnsToWrite(in));
	}

	@Override
	public void close() throws Exception {
		for (OutputFormat<?> format : formats.values()) {
			format.close();
		}
		formats.clear();
	}
}
