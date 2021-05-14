package org.apache.flink.runtime.jobgraph;

import org.apache.flink.api.common.cache.DistributedCache;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.util.FileUtils;
import org.apache.flink.util.FlinkRuntimeException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.util.Collection;

/**
 * Utilities for generating {@link JobGraph}.
 */
public enum JobGraphUtils {

	public static void addUserArtifactEntries(Collection<Tuple2<String, DistributedCache.DistributedCacheEntry>> userArtifacts, JobGraph jobGraph) {
		if (userArtifacts != null && !userArtifacts.isEmpty()) {
				java.nio.file.Path tmpDir = Files.createTempDirectory("flink-distributed-cache-" + jobGraph.getJobID());
				for (Tuple2<String, DistributedCache.DistributedCacheEntry> originalEntry : userArtifacts) {
					Path filePath = new Path(originalEntry.f1.filePath);
					boolean isLocalDir = false;
					FileSystem sourceFs = filePath.getFileSystem();
					isLocalDir = !sourceFs.isDistributedFS() && sourceFs.getFileStatus(filePath).isDir();
					// zip local directories because we only support file uploads
					DistributedCache.DistributedCacheEntry entry;
					if (isLocalDir) {
						Path zip = FileUtils.compressDirectory(filePath, new Path(tmpDir.toString(), filePath.getName() + ".zip"));
						entry = new DistributedCache.DistributedCacheEntry(zip.toString(), originalEntry.f1.isExecutable, true);
					} else {
						entry = new DistributedCache.DistributedCacheEntry(filePath.toString(), originalEntry.f1.isExecutable, false);
					}
					jobGraph.addUserArtifact(originalEntry.f0, entry);
				}
		}
	}
}
