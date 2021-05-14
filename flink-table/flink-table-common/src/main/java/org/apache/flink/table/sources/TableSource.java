package org.apache.flink.table.sources;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.utils.TableConnectorUtils;

import static org.apache.flink.table.types.utils.TypeConversions.fromLegacyInfoToDataType;

/**
 * Defines an external table with the schema that is provided by {@link TableSource#getTableSchema}.
 *
 * <p>The data of a {@link TableSource} is produced as a {@code DataSet} in case of a {@code BatchTableSource}
 * or as a {@code DataStream} in case of a {@code StreamTableSource}. The type of ths produced
 * {@code DataSet} or {@code DataStream} is specified by the {@link TableSource#getProducedDataType()} method.
 *
 * <p>By default, the fields of the {@link TableSchema} are implicitly mapped by name to the fields of
 * the produced {@link DataType}. An explicit mapping can be defined by implementing the
 * {@link DefinedFieldMapping} interface.
 *
 * @param <T> The return type of the {@link TableSource}.
 */
@PublicEvolving
public interface TableSource<T> {

	/**
	 * Returns the {@link DataType} for the produced data of the {@link TableSource}.
	 *
	 * @return The data type of the returned {@code DataSet} or {@code DataStream}.
	 */
	default DataType getProducedDataType() {
		final TypeInformation<T> legacyType = getReturnType();
	
		return fromLegacyInfoToDataType(legacyType);
	}

	/**
	 * @deprecated This method will be removed in future versions as it uses the old type system. It
	 *             is recommended to use {@link #getProducedDataType()} instead which uses the new type
	 *             system based on {@link DataTypes}. Please make sure to use either the old or the new type
	 *             system consistently to avoid unintended behavior. See the website documentation
	 *             for more information.
	 */
	@Deprecated
	default TypeInformation<T> getReturnType() {
		return null;
	}

	/**
	 * Returns the schema of the produced table.
	 *
	 * @return The {@link TableSchema} of the produced table.
	 * @deprecated Table schema is a logical description of a table and should not be part of the physical TableSource.
	 *             Define schema when registering a Table either in DDL or in {@code TableEnvironment#connect(...)}.
	 */
	@Deprecated
	TableSchema getTableSchema();

	/**
	 * Describes the table source.
	 *
	 * @return A String explaining the {@link TableSource}.
	 */
	default String explainSource() {
		return TableConnectorUtils.generateRuntimeName(getClass(), getTableSchema().getFieldNames());
	}
}
