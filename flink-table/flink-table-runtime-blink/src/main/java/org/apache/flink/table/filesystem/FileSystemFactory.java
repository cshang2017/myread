package org.apache.flink.table.filesystem;

import org.apache.flink.annotation.Internal;
import org.apache.flink.core.fs.FileSystem;

import java.io.IOException;
import java.io.Serializable;
import java.net.URI;

/**
 * A factory to create file systems.
 */
@Internal
public interface FileSystemFactory extends Serializable {

	/**
	 * Creates a new file system for the given file system URI.
	 * The URI describes the type of file system (via its scheme) and optionally the
	 * authority (for example the host) of the file system.
	 *
	 * @param fsUri The URI that describes the file system.
	 * @return A new instance of the specified file system.
	 *
	 * @throws IOException Thrown if the file system could not be instantiated.
	 */
	FileSystem create(URI fsUri) throws IOException;
}
