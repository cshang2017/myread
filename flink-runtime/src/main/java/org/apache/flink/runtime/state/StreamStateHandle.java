

package org.apache.flink.runtime.state;

import org.apache.flink.core.fs.FSDataInputStream;

import java.io.IOException;
import java.util.Optional;

/**
 * A {@link StateObject} that represents state that was written to a stream. The data can be read
 * back via {@link #openInputStream()}.
 */
public interface StreamStateHandle extends StateObject {

	/**
	 * Returns an {@link FSDataInputStream} that can be used to read back the data that
	 * was previously written to the stream.
	 */
	FSDataInputStream openInputStream() throws IOException;

	/**
	 * @return Content of this handle as bytes array if it is already in memory.
	 */
	Optional<byte[]> asBytesIfInMemory();
}
