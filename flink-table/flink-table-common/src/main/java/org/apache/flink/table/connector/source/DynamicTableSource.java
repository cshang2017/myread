package org.apache.flink.table.connector.source;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.RuntimeConverter;
import org.apache.flink.table.connector.source.abilities.SupportsComputedColumnPushDown;
import org.apache.flink.table.connector.source.abilities.SupportsFilterPushDown;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.types.Row;

import javax.annotation.Nullable;

import java.io.Serializable;

/**
 * Source of a dynamic table from an external storage system.
 *
 * <p>Dynamic tables are the core concept of Flink's Table & SQL API for processing both bounded and
 * unbounded data in a unified fashion. By definition, a dynamic table can change over time.
 *
 * <p>When reading a dynamic table, the content can either be considered as:
 * <ul>
 *     <li>A changelog (finite or infinite) for which all changes are consumed continuously until the
 *     changelog is exhausted. See {@link ScanTableSource} for more information.
 *     <li>A continuously changing or very large external table whose content is usually never read
 *     entirely but queried for individual values when necessary. See {@link LookupTableSource} for
 *     more information.
 * </ul>
 *
 * <p>Note: Both interfaces can be implemented at the same time. The planner decides about their usage
 * depending on the specified query.
 *
 * <p>Instances of the above mentioned interfaces can be seen as factories that eventually produce concrete
 * runtime implementation for reading the actual data.
 *
 * <p>Depending on the optionally declared abilities such as {@link SupportsComputedColumnPushDown} or
 * {@link SupportsFilterPushDown}, the planner might apply changes to an instance and thus mutates
 * the produced runtime implementation.
 */
@PublicEvolving
public interface DynamicTableSource {

	/**
	 * Creates a copy of this instance during planning. The copy should be a deep copy of all mutable
	 * members.
	 */
	DynamicTableSource copy();

	/**
	 * Returns a string that summarizes this source for printing to a console or log.
	 */
	String asSummaryString();

	// --------------------------------------------------------------------------------------------
	// Helper interfaces
	// --------------------------------------------------------------------------------------------

	/**
	 * Base context for creating runtime implementation via a {@link ScanTableSource.ScanRuntimeProvider}
	 * and {@link LookupTableSource.LookupRuntimeProvider}.
	 *
	 * <p>It offers utilities by the planner for creating runtime implementation with minimal dependencies
	 * to internal data structures.
	 *
	 * <p>Methods should be called in {@link ScanTableSource#getScanRuntimeProvider(ScanTableSource.ScanContext)}
	 * and {@link LookupTableSource#getLookupRuntimeProvider(LookupTableSource.LookupContext)}. The returned
	 * instances are {@link Serializable} and can be directly passed into the runtime implementation class.
	 */
	interface Context {

		/**
		 * Creates type information describing the internal data structures of the given {@link DataType}.
		 *
		 * @see TableSchema#toPhysicalRowDataType()
		 */
		TypeInformation<?> createTypeInformation(DataType producedDataType);

		/**
		 * Creates a converter for mapping between objects specified by the given {@link DataType} and
		 * Flink's internal data structures that can be passed into a runtime implementation.
		 *
		 * <p>For example, a {@link Row} and its fields can be converted into {@link RowData}, or a (possibly
		 * nested) POJO can be converted into the internal representation for structured types.
		 *
		 * @see LogicalType#supportsInputConversion(Class)
		 * @see TableSchema#toPhysicalRowDataType()
		 */
		DataStructureConverter createDataStructureConverter(DataType producedDataType);
	}

	/**
	 * Converter for mapping between objects and Flink's internal data structures during runtime.
	 *
	 * <p>On request, the planner will provide a specialized (possibly code generated) converter that
	 * can be passed into a runtime implementation.
	 *
	 * <p>For example, a {@link Row} and its fields can be converted into {@link RowData}, or a (possibly
	 * nested) POJO can be converted into the internal representation for structured types.
	 *
	 * @see LogicalType#supportsInputConversion(Class)
	 */
	interface DataStructureConverter extends RuntimeConverter {

		/**
		 * Converts the given object into an internal data structure.
		 */
		@Nullable Object toInternal(@Nullable Object externalStructure);
	}
}
