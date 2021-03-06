package org.apache.flink.table.planner.catalog;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableColumn;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogManager.TableLookupResult;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ConnectorCatalogTable;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.factories.TableFactoryUtil;
import org.apache.flink.table.factories.TableSourceFactory;
import org.apache.flink.table.factories.TableSourceFactoryContextImpl;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.plan.stats.FlinkStatistic;
import org.apache.flink.table.planner.sources.TableSourceUtil;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.table.sources.TableSource;
import org.apache.flink.table.sources.TableSourceValidation;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.TimestampKind;
import org.apache.flink.table.types.logical.TimestampType;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.TemporalTable;
import org.apache.calcite.schema.impl.AbstractTable;

import java.util.List;
import java.util.Optional;

/**
 * Represents a wrapper for {@link CatalogBaseTable} in {@link org.apache.calcite.schema.Schema}.
 *
 * <p>This table would be converted to
 * {@link org.apache.flink.table.planner.plan.schema.FlinkPreparingTableBase}
 * based on its internal source type during sql-to-rel conversion.
 *
 * <p>See
 * {@link org.apache.flink.table.planner.plan.FlinkCalciteCatalogReader#getTable(List)}
 * for details.
 */
public class CatalogSchemaTable extends AbstractTable implements TemporalTable {
	//~ Instance fields --------------------------------------------------------

	private final ObjectIdentifier tableIdentifier;
	private final TableLookupResult lookupResult;
	private final FlinkStatistic statistic;
	private final boolean isStreamingMode;
	private final Catalog catalog;

	//~ Constructors -----------------------------------------------------------

	/**
	 * Create a CatalogSchemaTable instance.
	 *
	 * @param tableIdentifier Table identifier
	 * @param lookupResult A result of catalog lookup
	 * @param statistic Table statistics
	 * @param catalog The catalog which the schema table belongs to
	 * @param isStreaming If the table is for streaming mode
	 */
	public CatalogSchemaTable(
			ObjectIdentifier tableIdentifier,
			TableLookupResult lookupResult,
			FlinkStatistic statistic,
			Catalog catalog,
			boolean isStreaming) {
		this.tableIdentifier = tableIdentifier;
		this.lookupResult = lookupResult;
		this.statistic = statistic;
		this.catalog = catalog;
		this.isStreamingMode = isStreaming;
	}

	
	public Catalog getCatalog() {
		return catalog;
	}

	public ObjectIdentifier getTableIdentifier() {
		return tableIdentifier;
	}

	public CatalogBaseTable getCatalogTable() {
		return lookupResult.getTable();
	}

	public boolean isTemporary() {
		return lookupResult.isTemporary();
	}

	public boolean isStreamingMode() {
		return isStreamingMode;
	}

	@Override
	public RelDataType getRowType(RelDataTypeFactory typeFactory) {

		//cshang understand this
		final FlinkTypeFactory flinkTypeFactory = (FlinkTypeFactory) typeFactory;
		TableSchema tableSchema = lookupResult.getResolvedSchema();
		final DataType[] fieldDataTypes = tableSchema.getFieldDataTypes();
		CatalogBaseTable catalogTable = lookupResult.getTable();
		if (!isStreamingMode
				&& catalogTable instanceof ConnectorCatalogTable
				&& ((ConnectorCatalogTable<?, ?>) catalogTable).getTableSource().isPresent()) {
			// If the table source is bounded, materialize the time attributes to normal TIMESTAMP type.
			// Now for ConnectorCatalogTable, there is no way to
			// deduce if it is bounded in the table environment, so the data types in TableSchema
			// always patched with TimeAttribute.
			// See ConnectorCatalogTable#calculateSourceSchema
			// for details.

			// Remove the patched time attributes type to let the TableSourceTable handle it.
			// We should remove this logic if the isBatch flag in ConnectorCatalogTable is fixed.
	
			for (int i = 0; i < fieldDataTypes.length; i++) {
				LogicalType lt = fieldDataTypes[i].getLogicalType();
				if (lt instanceof TimestampType
					&& (((TimestampType) lt).getKind() == TimestampKind.PROCTIME
					|| ((TimestampType) lt).getKind() == TimestampKind.ROWTIME)) {
					int precision = ((TimestampType) lt).getPrecision();
					fieldDataTypes[i] = DataTypes.TIMESTAMP(precision);
				}
			}
		}

		// The following block is a workaround to support tables defined by TableEnvironment.connect() and
		// the actual table sources implement DefinedProctimeAttribute/DefinedRowtimeAttributes.
		// It should be removed after we remove DefinedProctimeAttribute/DefinedRowtimeAttributes.
		Optional<TableSource<?>> sourceOpt = findAndCreateTableSource();
		if (isStreamingMode
			&& tableSchema.getTableColumns().stream().noneMatch(TableColumn::isGenerated)
			&& tableSchema.getWatermarkSpecs().isEmpty()
			&& sourceOpt.isPresent()) {
			TableSource<?> source = sourceOpt.get();
			if (TableSourceValidation.hasProctimeAttribute(source)
					|| TableSourceValidation.hasRowtimeAttribute(source)) {
				// If the table is defined by TableEnvironment.connect(), and use the legacy proctime and rowtime
				// descriptors, the TableSchema should fallback to ConnectorCatalogTable#calculateSourceSchema
				tableSchema = ConnectorCatalogTable.calculateSourceSchema(source, false);
			}
		}

		return TableSourceUtil.getSourceRowType(
			flinkTypeFactory,
			tableSchema,
			scala.Option.empty(),
			isStreamingMode);
	}

	@Override
	public FlinkStatistic getStatistic() {
		return statistic;
	}

	@Override
	public String getSysStartFieldName() {
		return "sys_start";
	}

	@Override
	public String getSysEndFieldName() {
		return "sys_end";
	}

	private Optional<TableSource<?>> findAndCreateTableSource() {
		Optional<TableSource<?>> tableSource = Optional.empty();

		if (lookupResult.getTable() instanceof CatalogTable) {
			// Use an empty config for TableSourceFactoryContextImpl since we can't fetch the
			// actual TableConfig here. And currently the empty config do not affect the logic.
			ReadableConfig config = new Configuration();
			TableSourceFactory.Context context =
				new TableSourceFactoryContextImpl(tableIdentifier, (CatalogTable) lookupResult.getTable(), config);
			TableSource<?> source = TableFactoryUtil.findAndCreateTableSource(context);
			if (source instanceof StreamTableSource) {
				tableSource = Optional.of(source);
			}
		}
		
		return tableSource;
	}
}
