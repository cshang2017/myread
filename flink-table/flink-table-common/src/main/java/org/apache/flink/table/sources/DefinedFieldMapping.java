package org.apache.flink.table.sources;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.types.DataType;

import javax.annotation.Nullable;

import java.util.Map;

/**
 * The {@link DefinedFieldMapping} interface provides a mapping for the fields of the table schema
 * ({@link TableSource#getTableSchema} to fields of the physical produced data type
 * {@link TableSource#getProducedDataType()} of a {@link TableSource}.
 *
 * <p>If a {@link TableSource} does not implement the {@link DefinedFieldMapping} interface, the fields of
 * its {@link TableSchema} are mapped to the fields of its produced {@link DataType} by name.
 *
 * <p>If the fields cannot or should not be implicitly mapped by name, an explicit mapping can be
 * provided by implementing this interface.
 *
 * <p>If a mapping is provided, all fields must be explicitly mapped.
 */
@PublicEvolving
public interface DefinedFieldMapping {

	/**
	 * Returns the mapping for the fields of the {@link TableSource}'s {@link TableSchema} to the fields of
	 * its produced {@link DataType}.
	 *
	 * <p>The mapping is done based on field names, e.g., a mapping "name" -> "f1" maps the schema field
	 * "name" to the field "f1" of the produced data type, for example in this case the second field of a
	 * {@link org.apache.flink.api.java.tuple.Tuple}.
	 *
	 * <p>The returned mapping must map all fields (except proctime and rowtime fields) to the produced data
	 * type. It can also provide a mapping for fields which are not in the {@link TableSchema} to make
	 * fields in the physical {@link DataType} accessible for a {@code TimestampExtractor}.
	 *
	 * @return A mapping from {@link TableSchema} fields to {@link DataType} fields or
	 * null if no mapping is necessary.
	 */
	@Nullable
	Map<String, String> getFieldMapping();
}
