

package org.apache.flink.table.filesystem;

import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;


/**
 * Partition commit policy to add success file to directory. Success file is configurable and
 * empty file.
 */
public class SuccessFileCommitPolicy implements PartitionCommitPolicy {


	private final String fileName;
	private final FileSystem fileSystem;

	public SuccessFileCommitPolicy(String fileName, FileSystem fileSystem) {
		this.fileName = fileName;
		this.fileSystem = fileSystem;
	}

	@Override
	public void commit(Context context) throws Exception {
		fileSystem.create(
				new Path(context.partitionPath(), fileName),
				FileSystem.WriteMode.OVERWRITE).close();
	}
}
