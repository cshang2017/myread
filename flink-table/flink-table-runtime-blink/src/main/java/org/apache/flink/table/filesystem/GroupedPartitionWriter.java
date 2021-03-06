
package org.apache.flink.table.filesystem;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.io.OutputFormat;

import static org.apache.flink.table.utils.PartitionPathUtils.generatePartitionPath;

/**
 * {@link PartitionWriter} for grouped dynamic partition inserting. It will create a new format
 * when partition changed.
 *
 * @param <T> The type of the consumed records.
 */
@Internal
public class GroupedPartitionWriter<T> implements PartitionWriter<T> {

	private final Context<T> context;
	private final PartitionTempFileManager manager;
	private final PartitionComputer<T> computer;

	private OutputFormat<T> currentFormat;
	private String currentPartition;

	public GroupedPartitionWriter(
			Context<T> context,
			PartitionTempFileManager manager,
			PartitionComputer<T> computer) {
		this.context = context;
		this.manager = manager;
		this.computer = computer;
	}

	@Override
	public void write(T in) throws Exception {
		String partition = generatePartitionPath(computer.generatePartValues(in));
		if (!partition.equals(currentPartition)) {
			if (currentFormat != null) {
				currentFormat.close();
			}

			currentFormat = context.createNewOutputFormat(manager.createPartitionDir(partition));
			currentPartition = partition;
		}
		currentFormat.writeRecord(computer.projectColumnsToWrite(in));
	}

	@Override
	public void close() throws Exception {
		if (currentFormat != null) {
			currentFormat.close();
			currentFormat = null;
		}
	}
}
