package org.apache.flink.table.factories;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.common.serialization.BulkWriter;
import org.apache.flink.api.common.serialization.Encoder;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * File system format factory for creating configured instances of reader and writer.
 */
@Internal
public interface FileSystemFormatFactory extends Factory {

	/**
	 * Create {@link InputFormat} reader.
	 */
	InputFormat<RowData, ?> createReader(ReaderContext context);

	/**
	 * Create {@link Encoder} writer.
	 */
	Optional<Encoder<RowData>> createEncoder(WriterContext context);

	/**
	 * Create {@link BulkWriter.Factory} writer.
	 */
	Optional<BulkWriter.Factory<RowData>> createBulkWriterFactory(WriterContext context);

	/**
	 * Context of {@link #createReader}.
	 */
	interface ReaderContext {

		/**
		 * Full schema of the table.
		 */
		TableSchema getSchema();

		/**
		 * Options of this format.
		 */
		ReadableConfig getFormatOptions();

		/**
		 * Partition keys of the table.
		 */
		List<String> getPartitionKeys();

		/**
		 * The default partition name in case the dynamic partition column value is
		 * null/empty string.
		 */
		String getDefaultPartName();

		/**
		 * Read paths.
		 */
		Path[] getPaths();

		/**
		 * Project the fields of the reader returned.
		 */
		int[] getProjectFields();

		/**
		 * Limiting push-down to reader. Reader only needs to try its best to limit the number
		 * of output records, but does not need to guarantee that the number must be less than or
		 * equal to the limit.
		 */
		long getPushedDownLimit();

		/**
		 * Pushed down filters, reader can try its best to filter records.
		 * The follow up operator will filter the records again.
		 */
		List<Expression> getPushedDownFilters();

		/**
		 * Get field names without partition keys.
		 */
		default String[] getFormatFieldNames() {
			return Arrays.stream(getSchema().getFieldNames())
				.filter(name -> !getPartitionKeys().contains(name))
				.toArray(String[]::new);
		}

		/**
		 * Get field types without partition keys.
		 */
		default DataType[] getFormatFieldTypes() {
			return Arrays.stream(getSchema().getFieldNames())
				.filter(name -> !getPartitionKeys().contains(name))
				.map(name -> getSchema().getFieldDataType(name).get())
				.toArray(DataType[]::new);
		}

		/**
		 * RowType of table that excludes partition key fields.
		 */
		default RowType getFormatRowType() {
			return RowType.of(
				Arrays.stream(getFormatFieldTypes())
					.map(DataType::getLogicalType)
					.toArray(LogicalType[]::new),
				getFormatFieldNames());
		}

		/**
		 * Mapping from non-partition project fields index to all project fields index.
		 */
		default List<String> getFormatProjectFields() {
			final List<String> selectFieldNames = Arrays.stream(getProjectFields())
				.mapToObj(i -> getSchema().getFieldNames()[i])
				.collect(Collectors.toList());
			return selectFieldNames.stream()
				.filter(name -> !getPartitionKeys().contains(name))
				.collect(Collectors.toList());
		}
	}

	/**
	 * Context of {@link #createEncoder} and {@link #createBulkWriterFactory}.
	 */
	interface WriterContext {

		/**
		 * Full schema of the table.
		 */
		TableSchema getSchema();

		/**
		 * Options of this format.
		 */
		ReadableConfig getFormatOptions();

		/**
		 * Partition keys of the table.
		 */
		List<String> getPartitionKeys();

		/**
		 * Get field names without partition keys.
		 */
		default String[] getFormatFieldNames() {
			return Arrays.stream(getSchema().getFieldNames())
					.filter(name -> !getPartitionKeys().contains(name))
					.toArray(String[]::new);
		}

		/**
		 * Get field types without partition keys.
		 */
		default DataType[] getFormatFieldTypes() {
			return Arrays.stream(getSchema().getFieldNames())
					.filter(name -> !getPartitionKeys().contains(name))
					.map(name -> getSchema().getFieldDataType(name).get())
					.toArray(DataType[]::new);
		}

		/**
		 * Get RowType of the table without partition keys.
		 * @return
		 */
		default RowType getFormatRowType() {
			return RowType.of(
				Arrays.stream(getFormatFieldTypes())
					.map(DataType::getLogicalType)
					.toArray(LogicalType[]::new),
				getFormatFieldNames());
		}
	}
}
