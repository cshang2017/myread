package org.apache.flink.table.filesystem;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.io.OutputFormat;

import java.io.IOException;
import java.util.LinkedHashMap;

import static org.apache.flink.table.utils.PartitionPathUtils.generatePartitionPath;

/**
 * {@link PartitionWriter} for single directory writer. It just use one format to write.
 *
 * @param <T> The type of the consumed records.
 */
@Internal
public class SingleDirectoryWriter<T> implements PartitionWriter<T> {

	private final Context<T> context;
	private final PartitionTempFileManager manager;
	private final PartitionComputer<T> computer;
	private final LinkedHashMap<String, String> staticPartitions;

	private OutputFormat<T> format;

	public SingleDirectoryWriter(
			Context<T> context,
			PartitionTempFileManager manager,
			PartitionComputer<T> computer,
			LinkedHashMap<String, String> staticPartitions) {
		this.context = context;
		this.manager = manager;
		this.computer = computer;
		this.staticPartitions = staticPartitions;
	}

	private void createFormat() throws IOException {
		this.format = context.createNewOutputFormat(staticPartitions.size() == 0 ?
				manager.createPartitionDir() :
				manager.createPartitionDir(generatePartitionPath(staticPartitions)));
	}

	@Override
	public void write(T in) throws Exception {
		if (format == null) {
			createFormat();
		}
		format.writeRecord(computer.projectColumnsToWrite(in));
	}

	@Override
	public void close() throws Exception {
		if (format != null) {
			format.close();
			format = null;
		}
	}
}
