package org.apache.flink.table.api.internal;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.TableColumn;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.delegation.Parser;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeFamily;
import org.apache.flink.table.types.logical.TimestampKind;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks;
import org.apache.flink.table.types.utils.TypeConversions;

/**
 * The {@link CatalogTableSchemaResolver} is used to derive correct result type of computed column,
 * because the date type of computed column from catalog table is not trusted.
 *
 * <p>Such as `proctime()` function, its type in given TableSchema is Timestamp(3),
 * but its correct type is Timestamp(3) *PROCTIME*.
 */
@Internal
public class CatalogTableSchemaResolver {
	private final Parser parser;
	// A flag to indicate the table environment should work in a batch or streaming
	// TODO remove this once FLINK-18180 is finished
	private final boolean isStreamingMode;

	public CatalogTableSchemaResolver(Parser parser, boolean isStreamingMode) {
		this.parser = parser;
		this.isStreamingMode = isStreamingMode;
	}

	/**
	 * Resolve the computed column's type for the given schema.
	 *
	 * @param tableSchema Table schema to derive table field names and data types
	 * @return the resolved TableSchema
	 */
	public TableSchema resolve(TableSchema tableSchema) {
		final String rowtime;
		if (!tableSchema.getWatermarkSpecs().isEmpty()) {
			// TODO: [FLINK-14473] we only support top-level rowtime attribute right now
			rowtime = tableSchema.getWatermarkSpecs().get(0).getRowtimeAttribute();
		} else {
			rowtime = null;
		}

		String[] fieldNames = tableSchema.getFieldNames();
		DataType[] fieldTypes = tableSchema.getFieldDataTypes();

		TableSchema.Builder builder = TableSchema.builder();
		for (int i = 0; i < tableSchema.getFieldCount(); ++i) {
			TableColumn tableColumn = tableSchema.getTableColumns().get(i);
			DataType fieldType = fieldTypes[i];

			if (tableColumn.isGenerated()) {
				fieldType = resolveExpressionDataType(tableColumn.getExpr().get(), tableSchema);
				if (isProctime(fieldType)) {
					if (fieldNames[i].equals(rowtime)) {
						throw new TableException("Watermark can not be defined for a processing time attribute column.");
					}
				}
			}

			if (isStreamingMode && fieldNames[i].equals(rowtime)) {
				TimestampType originalType = (TimestampType) fieldType.getLogicalType();
				LogicalType rowtimeType = new TimestampType(
						originalType.isNullable(),
						TimestampKind.ROWTIME,
						originalType.getPrecision());
				fieldType = TypeConversions.fromLogicalToDataType(rowtimeType);
			}

			if (tableColumn.isGenerated()) {
				builder.field(fieldNames[i], fieldType, tableColumn.getExpr().get());
			} else {
				builder.field(fieldNames[i], fieldType);
			}
		}

		tableSchema.getWatermarkSpecs().forEach(builder::watermark);
		tableSchema.getPrimaryKey().ifPresent(
				pk -> builder.primaryKey(pk.getName(), pk.getColumns().toArray(new String[0])));
		return builder.build();
	}

	private boolean isProctime(DataType exprType) {
		return LogicalTypeChecks.hasFamily(exprType.getLogicalType(), LogicalTypeFamily.TIMESTAMP) &&
			LogicalTypeChecks.isProctimeAttribute(exprType.getLogicalType());
	}

	private DataType resolveExpressionDataType(String expr, TableSchema tableSchema) {
		ResolvedExpression resolvedExpr = parser.parseSqlExpression(expr, tableSchema);
		
		return resolvedExpr.getOutputDataType();
	}
}
