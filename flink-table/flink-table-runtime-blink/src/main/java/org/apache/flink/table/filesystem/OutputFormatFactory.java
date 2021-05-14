package org.apache.flink.table.filesystem;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.core.fs.Path;

import java.io.Serializable;

/**
 * A factory to create an {@link OutputFormat}.
 *
 * @param <T> The type of the consumed records.
 */
@Internal
public interface OutputFormatFactory<T> extends Serializable {

	/**
	 * Create a {@link OutputFormat} with specific path.
	 */
	OutputFormat<T> createOutputFormat(Path path);
}
