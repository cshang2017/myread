package org.apache.flink.table.sinks;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.types.DataType;

import static org.apache.flink.table.types.utils.TypeConversions.fromLegacyInfoToDataType;

/**
 * A {@link TableSink} specifies how to emit a table to an external
 * system or location.
 *
 * <p>The interface is generic such that it can support different storage locations and formats.
 *
 * @param <T> The return type of the {@link TableSink}.
 */
@PublicEvolving
public interface TableSink<T> {

	/**
	 * Returns the data type consumed by this {@link TableSink}.
	 *
	 * @return The data type expected by this {@link TableSink}.
	 */
	default DataType getConsumedDataType() {
		final TypeInformation<T> legacyType = getOutputType();
		if (legacyType == null) {
			throw new TableException("Table sink does not implement a consumed data type.");
		}
		return fromLegacyInfoToDataType(legacyType);
	}

	/**
	 * @deprecated This method will be removed in future versions as it uses the old type system. It
	 *             is recommended to use {@link #getConsumedDataType()} instead which uses the new type
	 *             system based on {@link DataTypes}. Please make sure to use either the old or the new type
	 *             system consistently to avoid unintended behavior. See the website documentation
	 *             for more information.
	 */
	@Deprecated
	default TypeInformation<T> getOutputType() {
		return null;
	}

	/**
	 * Returns the schema of the consumed table.
	 *
	 * @return The {@link TableSchema} of the consumed table.
	 */
	default TableSchema getTableSchema() {
		final String[] fieldNames = getFieldNames();
		final TypeInformation[] legacyFieldTypes = getFieldTypes();
		
		return new TableSchema(fieldNames, legacyFieldTypes);
	}

	/**
	 * @deprecated Use the field names of {@link #getTableSchema()} instead.
	 */
	@Deprecated
	default String[] getFieldNames() {
		return null;
	}

	/**
	 * @deprecated Use the field types of {@link #getTableSchema()} instead.
	 */
	@Deprecated
	default TypeInformation<?>[] getFieldTypes() {
		return null;
	}

	/**
	 * Returns a copy of this {@link TableSink} configured with the field names and types of the
	 * table to emit.
	 *
	 * @param fieldNames The field names of the table to emit.
	 * @param fieldTypes The field types of the table to emit.
	 * @return A copy of this {@link TableSink} configured with the field names and types of the
	 *         table to emit.
	 *
	 * @deprecated This method will be dropped in future versions. It is recommended to pass a
	 *             static schema when instantiating the sink instead.
	 */
	@Deprecated
	TableSink<T> configure(String[] fieldNames, TypeInformation<?>[] fieldTypes);
}
