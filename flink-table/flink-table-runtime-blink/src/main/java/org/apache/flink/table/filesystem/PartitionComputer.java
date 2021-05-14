package org.apache.flink.table.filesystem;

import org.apache.flink.annotation.Internal;

import java.io.Serializable;
import java.util.LinkedHashMap;

/**
 * Compute partition path from record and project non-partition columns for output writer.
 *
 * <p>See {@link RowPartitionComputer}.
 *
 * @param <T> The type of the consumed records.
 */
@Internal
public interface PartitionComputer<T> extends Serializable {

	/**
	 * Compute partition values from record.
	 *
	 * @param in input record.
	 * @return partition values.
	 */
	LinkedHashMap<String, String> generatePartValues(T in) throws Exception;

	/**
	 * Project non-partition columns for output writer.
	 *
	 * @param in input record.
	 * @return projected record.
	 */
	T projectColumnsToWrite(T in) throws Exception;
}
