package org.apache.flink.table.planner.utils;

import org.apache.flink.sql.parser.ddl.SqlAddReplaceColumns;
import org.apache.flink.sql.parser.ddl.SqlChangeColumn;
import org.apache.flink.sql.parser.ddl.SqlTableColumn;
import org.apache.flink.sql.parser.ddl.SqlTableOption;
import org.apache.flink.table.api.TableColumn;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.api.WatermarkSpec;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.CatalogTableImpl;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.ddl.AlterTableSchemaOperation;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.utils.TypeConversions;

import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.validate.SqlValidator;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Utils methods for converting sql to operations.
 */
public class OperationConverterUtils {

	private OperationConverterUtils() {
	}

	public static Operation convertAddReplaceColumns(
			ObjectIdentifier tableIdentifier,
			SqlAddReplaceColumns addReplaceColumns,
			CatalogTable catalogTable,
			SqlValidator sqlValidator) {
		// This is only used by the Hive dialect at the moment. In Hive, only non-partition columns can be
		// added/replaced and users will only define non-partition columns in the new column list. Therefore, we require
		// that partitions columns must appear last in the schema (which is inline with Hive). Otherwise, we won't be
		// able to determine the column positions after the non-partition columns are replaced.
		TableSchema oldSchema = catalogTable.getSchema();
		int numPartCol = catalogTable.getPartitionKeys().size();
		Set<String> lastCols = oldSchema.getTableColumns()
				.subList(oldSchema.getFieldCount() - numPartCol, oldSchema.getFieldCount())
				.stream().map(TableColumn::getName).collect(Collectors.toSet());
		if (!lastCols.equals(new HashSet<>(catalogTable.getPartitionKeys()))) {
			throw new ValidationException("ADD/REPLACE COLUMNS on partitioned tables requires partition columns to appear last");
		}

		// set non-partition columns
		TableSchema.Builder builder = TableSchema.builder();
		if (!addReplaceColumns.isReplace()) {
			List<TableColumn> nonPartCols = oldSchema.getTableColumns().subList(0, oldSchema.getFieldCount() - numPartCol);
			for (TableColumn column : nonPartCols) {
				builder.add(column);
			}
			setWatermarkAndPK(builder, catalogTable.getSchema());
		}
		for (SqlNode sqlNode : addReplaceColumns.getNewColumns()) {
			builder.add(toTableColumn((SqlTableColumn) sqlNode, sqlValidator));
		}

		// set partition columns
		List<TableColumn> partCols = oldSchema.getTableColumns().subList(oldSchema.getFieldCount() - numPartCol, oldSchema.getFieldCount());
		for (TableColumn column : partCols) {
			builder.add(column);
		}

		// set properties
		Map<String, String> newProperties = new HashMap<>(catalogTable.getOptions());
		newProperties.putAll(extractProperties(addReplaceColumns.getProperties()));

		return new AlterTableSchemaOperation(
				tableIdentifier,
				new CatalogTableImpl(
						builder.build(),
						catalogTable.getPartitionKeys(),
						newProperties,
						catalogTable.getComment())
		);
	}

	public static Operation convertChangeColumn(
			ObjectIdentifier tableIdentifier,
			SqlChangeColumn changeColumn,
			CatalogTable catalogTable,
			SqlValidator sqlValidator) {
		String oldName = changeColumn.getOldName().getSimple();
		if (catalogTable.getPartitionKeys().indexOf(oldName) >= 0) {
			// disallow changing partition columns
			throw new ValidationException("CHANGE COLUMN cannot be applied to partition columns");
		}
		TableSchema oldSchema = catalogTable.getSchema();
		int oldIndex = Arrays.asList(oldSchema.getFieldNames()).indexOf(oldName);
		if (oldIndex < 0) {
			throw new ValidationException(String.format("Old column %s not found for CHANGE COLUMN", oldName));
		}
		boolean first = changeColumn.isFirst();
		String after = changeColumn.getAfter() == null ? null : changeColumn.getAfter().getSimple();
		List<TableColumn> tableColumns = oldSchema.getTableColumns();
		TableColumn newTableColumn = toTableColumn(changeColumn.getNewColumn(), sqlValidator);
		if ((!first && after == null) || oldName.equals(after)) {
			tableColumns.set(oldIndex, newTableColumn);
		} else {
			// need to change column position
			tableColumns.remove(oldIndex);
			if (first) {
				tableColumns.add(0, newTableColumn);
			} else {
				int newIndex = tableColumns
						.stream()
						.map(TableColumn::getName)
						.collect(Collectors.toList())
						.indexOf(after);
				if (newIndex < 0) {
					throw new ValidationException(String.format("After column %s not found for CHANGE COLUMN", after));
				}
				tableColumns.add(newIndex + 1, newTableColumn);
			}
		}
		TableSchema.Builder builder = TableSchema.builder();
		for (TableColumn column : tableColumns) {
			builder.add(column);
		}
		setWatermarkAndPK(builder, oldSchema);
		TableSchema newSchema = builder.build();
		Map<String, String> newProperties = new HashMap<>(catalogTable.getOptions());
		newProperties.putAll(extractProperties(changeColumn.getProperties()));
		return new AlterTableSchemaOperation(
				tableIdentifier,
				new CatalogTableImpl(
						newSchema,
						catalogTable.getPartitionKeys(),
						newProperties,
						catalogTable.getComment()));
		// TODO: handle watermark and constraints
	}

	private static TableColumn toTableColumn(SqlTableColumn sqlTableColumn, SqlValidator sqlValidator) {
		String name = sqlTableColumn.getName().getSimple();
		SqlDataTypeSpec typeSpec = sqlTableColumn.getType();
		LogicalType logicalType = FlinkTypeFactory.toLogicalType(
				typeSpec.deriveType(sqlValidator, typeSpec.getNullable()));
		DataType dataType = TypeConversions.fromLogicalToDataType(logicalType);
		return TableColumn.of(name, dataType);
	}

	private static void setWatermarkAndPK(TableSchema.Builder builder, TableSchema schema) {
		for (WatermarkSpec watermarkSpec : schema.getWatermarkSpecs()) {
			builder.watermark(watermarkSpec);
		}
		schema.getPrimaryKey().ifPresent(pk -> {
			builder.primaryKey(pk.getName(), pk.getColumns().toArray(new String[0]));
		});
	}

	public static Map<String, String> extractProperties(SqlNodeList propList) {
		Map<String, String> properties = new HashMap<>();
		if (propList != null) {
			propList.getList().forEach(p ->
					properties.put(((SqlTableOption) p).getKeyString(), ((SqlTableOption) p).getValueString()));
		}
		return properties;
	}
}
