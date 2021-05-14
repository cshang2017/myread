package org.apache.flink.api.common.cache;

import org.apache.flink.annotation.Public;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ConfigurationUtils;
import org.apache.flink.core.fs.Path;

import java.io.File;
import java.io.Serializable;
import java.net.URI;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

/**
 * DistributedCache provides static methods to write the registered cache files into job configuration or decode
 * them from job configuration. It also provides user access to the file locally.
 */
@Public
public class DistributedCache {

	/**
	 * Meta info about an entry in {@link DistributedCache}.
	 *
	 * <p>Entries have different semantics for local directories depending on where we are in the job-submission process.
	 * After registration through the API {@code filePath} denotes the original directory.
	 * After the upload to the cluster (which includes zipping the directory), {@code filePath} denotes the (server-side) copy of the zip.
	 */
	public static class DistributedCacheEntry implements Serializable {

		public String filePath;
		public Boolean isExecutable;
		public boolean isZipped;

		public byte[] blobKey;

		/** Client-side constructor used by the API for initial registration. */
		public DistributedCacheEntry(String filePath, Boolean isExecutable) {
			this(filePath, isExecutable, null);
		}

		/** Client-side constructor used during job-submission for zipped directory. */
		public DistributedCacheEntry(String filePath, boolean isExecutable, boolean isZipped) {
			this(filePath, isExecutable, null, isZipped);
		}

		/** Server-side constructor used during job-submission for zipped directories. */
		public DistributedCacheEntry(String filePath, Boolean isExecutable, byte[] blobKey, boolean isZipped) {
			this.filePath = filePath;
			this.isExecutable = isExecutable;
			this.blobKey = blobKey;
			this.isZipped = isZipped;
		}

		/** Server-side constructor used during job-submission for files. */
		public DistributedCacheEntry(String filePath, Boolean isExecutable, byte[] blobKey){
			this(filePath, isExecutable, blobKey, false);
		}


		@Override
		public String toString() {
			return "DistributedCacheEntry{" +
				"filePath='" + filePath + '\'' +
				", isExecutable=" + isExecutable +
				", isZipped=" + isZipped +
				", blobKey=" + Arrays.toString(blobKey) +
				'}';
		}
	}

	// ------------------------------------------------------------------------

	private final Map<String, Future<Path>> cacheCopyTasks;

	public DistributedCache(Map<String, Future<Path>> cacheCopyTasks) {
		this.cacheCopyTasks = cacheCopyTasks;
	}

	// ------------------------------------------------------------------------

	public File getFile(String name) {
		Future<Path> future = cacheCopyTasks.get(name);
		final Path path = future.get();
		URI tmp = path.makeQualified(path.getFileSystem()).toUri();
		
		return new File(tmp);
	}

	// ------------------------------------------------------------------------
	//  Utilities to read/write cache files from/to the configuration
	// ------------------------------------------------------------------------

	public static void writeFileInfoToConfig(String name, DistributedCacheEntry e, Configuration conf) {
		int num = conf.getInteger(CACHE_FILE_NUM, 0) + 1;
		conf.setInteger(CACHE_FILE_NUM, num);
		conf.setString(CACHE_FILE_NAME + num, name);
		conf.setString(CACHE_FILE_PATH + num, e.filePath);
		conf.setBoolean(CACHE_FILE_EXE + num, e.isExecutable || new File(e.filePath).canExecute());
		conf.setBoolean(CACHE_FILE_DIR + num, e.isZipped || new File(e.filePath).isDirectory());
		if (e.blobKey != null) {
			conf.setBytes(CACHE_FILE_BLOB_KEY + num, e.blobKey);
		}
	}

	public static Set<Entry<String, DistributedCacheEntry>> readFileInfoFromConfig(Configuration conf) {
		int num = conf.getInteger(CACHE_FILE_NUM, 0);
		if (num == 0) {
			return Collections.emptySet();
		}

		Map<String, DistributedCacheEntry> cacheFiles = new HashMap<String, DistributedCacheEntry>();
		for (int i = 1; i <= num; i++) {
			String name = conf.getString(CACHE_FILE_NAME + i, null);
			String filePath = conf.getString(CACHE_FILE_PATH + i, null);
			boolean isExecutable = conf.getBoolean(CACHE_FILE_EXE + i, false);
			boolean isDirectory = conf.getBoolean(CACHE_FILE_DIR + i, false);

			byte[] blobKey = conf.getBytes(CACHE_FILE_BLOB_KEY + i, null);
			cacheFiles.put(name, new DistributedCacheEntry(filePath, isExecutable, blobKey, isDirectory));
		}
		return cacheFiles.entrySet();
	}

	/**
	 * Parses a list of distributed cache entries encoded in a string. Can be used to parse a config option
	 * described by {@link org.apache.flink.configuration.PipelineOptions#CACHED_FILES}.
	 *
	 * <p>See {@link org.apache.flink.configuration.PipelineOptions#CACHED_FILES} for the format.
	 *
	 * @param files List of string encoded distributed cache entries.
	 */
	public static List<Tuple2<String, DistributedCacheEntry>> parseCachedFilesFromString(List<String> files) {
		return files.stream()
			.map(ConfigurationUtils::parseMap)
			.map(m -> Tuple2.of(
				m.get("name"),
				new DistributedCacheEntry(
					m.get("path"),
					Optional.ofNullable(m.get("executable")).map(Boolean::parseBoolean).orElse(false)))
			).collect(Collectors.toList());
	}

	private static final String CACHE_FILE_NUM = "DISTRIBUTED_CACHE_FILE_NUM";

	private static final String CACHE_FILE_NAME = "DISTRIBUTED_CACHE_FILE_NAME_";

	private static final String CACHE_FILE_PATH = "DISTRIBUTED_CACHE_FILE_PATH_";

	private static final String CACHE_FILE_EXE = "DISTRIBUTED_CACHE_FILE_EXE_";

	private static final String CACHE_FILE_DIR = "DISTRIBUTED_CACHE_FILE_DIR_";

	private static final String CACHE_FILE_BLOB_KEY = "DISTRIBUTED_CACHE_FILE_BLOB_KEY_";
}
