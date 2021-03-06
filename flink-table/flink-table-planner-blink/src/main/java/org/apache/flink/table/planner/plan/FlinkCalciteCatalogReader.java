package org.apache.flink.table.planner.plan;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.CatalogView;
import org.apache.flink.table.catalog.ConnectorCatalogTable;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.QueryOperationCatalogView;
import org.apache.flink.table.descriptors.ConnectorDescriptorValidator;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.factories.TableFactoryUtil;
import org.apache.flink.table.planner.catalog.CatalogSchemaTable;
import org.apache.flink.table.planner.catalog.QueryOperationCatalogViewTable;
import org.apache.flink.table.planner.catalog.SqlCatalogViewTable;
import org.apache.flink.table.planner.plan.schema.CatalogSourceTable;
import org.apache.flink.table.planner.plan.schema.FlinkPreparingTableBase;
import org.apache.flink.table.planner.plan.schema.LegacyCatalogSourceTable;
import org.apache.flink.table.planner.plan.schema.LegacyTableSourceTable;
import org.apache.flink.table.planner.plan.stats.FlinkStatistic;
import org.apache.flink.table.sources.LookupableTableSource;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.table.sources.TableSource;

import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.validate.SqlNameMatchers;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Flink specific {@link CalciteCatalogReader} that changes the RelOptTable which wrapped a
 * {@link CatalogSchemaTable} to a {@link FlinkPreparingTableBase}.
 */
public class FlinkCalciteCatalogReader extends CalciteCatalogReader {

	public FlinkCalciteCatalogReader(
		CalciteSchema rootSchema,
		List<List<String>> defaultSchemas,
		RelDataTypeFactory typeFactory,
		CalciteConnectionConfig config) {

		super(
			rootSchema,
			SqlNameMatchers.withCaseSensitive(config != null && config.caseSensitive()),
				Stream.concat(
					defaultSchemas.stream(),
					Stream.of(Collections.<String>emptyList())
				).collect(Collectors.toList()),
			typeFactory,
			config);
	}

	@Override
	public Prepare.PreparingTable getTable(List<String> names) {
		Prepare.PreparingTable originRelOptTable = super.getTable(names);
		if (originRelOptTable == null) {
			return null;
		} else {
			// Wrap as FlinkPreparingTableBase to use in query optimization.
			CatalogSchemaTable table = originRelOptTable.unwrap(CatalogSchemaTable.class);
			if (table != null) {
				return toPreparingTable(originRelOptTable.getRelOptSchema(),
					originRelOptTable.getQualifiedName(),
					originRelOptTable.getRowType(),
					table);
			} else {
				return originRelOptTable;
			}
		}
	}

	/**
	 * Translate this {@link CatalogSchemaTable} into Flink source table.
	 */
	private static FlinkPreparingTableBase toPreparingTable(
			RelOptSchema relOptSchema,
			List<String> names,
			RelDataType rowType,
			CatalogSchemaTable schemaTable) {
		final CatalogBaseTable baseTable = schemaTable.getCatalogTable();
		if (baseTable instanceof QueryOperationCatalogView) {
			return convertQueryOperationView(relOptSchema,
				names,
				rowType,
				(QueryOperationCatalogView) baseTable);
		} else if (baseTable instanceof ConnectorCatalogTable) {
			ConnectorCatalogTable<?, ?> connectorTable = (ConnectorCatalogTable<?, ?>) baseTable;
			if ((connectorTable).getTableSource().isPresent()) {
				return convertSourceTable(relOptSchema,
					rowType,
					schemaTable.getTableIdentifier(),
					connectorTable,
					schemaTable.getStatistic(),
					schemaTable.isStreamingMode());
			} else {
				throw new ValidationException("Cannot convert a connector table " +
					"without source.");
			}
		} else if (baseTable instanceof CatalogView) {
			return convertCatalogView(
				relOptSchema,
				names,
				rowType,
				schemaTable.getStatistic(),
				(CatalogView) baseTable);
		} else if (baseTable instanceof CatalogTable) {
			return convertCatalogTable(relOptSchema,
				names,
				rowType,
				(CatalogTable) baseTable,
				schemaTable);
		} else {
			throw new ValidationException("Unsupported table type: " + baseTable);
		}
	}

	private static FlinkPreparingTableBase convertQueryOperationView(
			RelOptSchema relOptSchema,
			List<String> names,
			RelDataType rowType,
			QueryOperationCatalogView view) {
		return QueryOperationCatalogViewTable.create(relOptSchema, names, rowType, view);
	}

	private static FlinkPreparingTableBase convertCatalogView(
			RelOptSchema relOptSchema,
			List<String> names,
			RelDataType rowType,
			FlinkStatistic statistic,
			CatalogView view) {
		return new SqlCatalogViewTable(relOptSchema, rowType, names, statistic, view, names.subList(0, 2));
	}

	private static FlinkPreparingTableBase convertSourceTable(
			RelOptSchema relOptSchema,
			RelDataType rowType,
			ObjectIdentifier tableIdentifier,
			ConnectorCatalogTable<?, ?> table,
			FlinkStatistic statistic,
			boolean isStreamingMode) {
		TableSource<?> tableSource = table.getTableSource().get();
		if (!(tableSource instanceof StreamTableSource ||
			tableSource instanceof LookupableTableSource)) {
			throw new ValidationException(
				"Only StreamTableSource and LookupableTableSource can be used in Blink planner.");
		}
		if (!isStreamingMode && tableSource instanceof StreamTableSource &&
			!((StreamTableSource<?>) tableSource).isBounded()) {
			throw new ValidationException("Only bounded StreamTableSource can be used in batch mode.");
		}

		return new LegacyTableSourceTable<>(
			relOptSchema,
			tableIdentifier,
			rowType,
			statistic,
			tableSource,
			isStreamingMode,
			table);
	}

	private static FlinkPreparingTableBase convertCatalogTable(
			RelOptSchema relOptSchema,
			List<String> names,
			RelDataType rowType,
			CatalogTable catalogTable,
			CatalogSchemaTable schemaTable) {
		if (isLegacySourceOptions(catalogTable, schemaTable)) {
			return new LegacyCatalogSourceTable<>(
				relOptSchema,
				names,
				rowType,
				schemaTable,
				catalogTable);
		} else {
			return new CatalogSourceTable<>(
				relOptSchema,
				names,
				rowType,
				schemaTable,
				catalogTable);
		}
	}

	/**
	 * Checks whether the {@link CatalogTable} uses legacy connector source options.
	 */
	private static boolean isLegacySourceOptions(
			CatalogTable catalogTable,
			CatalogSchemaTable schemaTable) {
		// normalize option keys
		DescriptorProperties properties = new DescriptorProperties(true);
		properties.putProperties(catalogTable.getOptions());
		if (properties.containsKey(ConnectorDescriptorValidator.CONNECTOR_TYPE)) {
			return true;
		} else {
			// try to create legacy table source using the options,
			// some legacy factories uses the new 'connector' key
			try {
				TableFactoryUtil.findAndCreateTableSource(
					schemaTable.getCatalog(),
					schemaTable.getTableIdentifier(),
					catalogTable,
					new Configuration());
				// success, then we will use the legacy factories
				return true;
			} catch (Throwable e) {
				// fail, then we will use new factories
				return false;
			}
		}
	}
}
