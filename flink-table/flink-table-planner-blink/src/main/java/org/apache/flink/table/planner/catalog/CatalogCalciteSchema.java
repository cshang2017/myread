package org.apache.flink.table.planner.catalog;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.catalog.CatalogManager;

import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Schemas;
import org.apache.calcite.schema.Table;

import java.util.HashSet;
import java.util.Set;

/**
 * A mapping between Flink's catalog and Calcite's schema. This enables to look up and access objects(tables, views,
 * functions, types) in SQL queries without registering them in advance. Databases are registered as sub-schemas
 * in the schema.
 */
@Internal
public class CatalogCalciteSchema extends FlinkSchema {

	private final String catalogName;
	private final CatalogManager catalogManager;
	// Flag that tells if the current planner should work in a batch or streaming mode.
	private final boolean isStreamingMode;

	public CatalogCalciteSchema(
			String catalogName,
			CatalogManager catalog,
			boolean isStreamingMode) {
		this.catalogName = catalogName;
		this.catalogManager = catalog;
		this.isStreamingMode = isStreamingMode;
	}

	/**
	 * Look up a sub-schema (database) by the given sub-schema name.
	 *
	 * @param schemaName name of sub-schema to look up
	 * @return the sub-schema with a given database name, or null
	 */
	@Override
	public Schema getSubSchema(String schemaName) {
		if (catalogManager.schemaExists(catalogName, schemaName)) {
			return new DatabaseCalciteSchema(schemaName, catalogName, catalogManager, isStreamingMode);
		} else {
			return null;
		}
	}

	@Override
	public Set<String> getSubSchemaNames() {
		return catalogManager.listSchemas(catalogName);
	}

	@Override
	public Table getTable(String name) {
		return null;
	}

	@Override
	public Set<String> getTableNames() {
		return new HashSet<>();
	}

	@Override
	public Expression getExpression(SchemaPlus parentSchema, String name) {
		return  Schemas.subSchemaExpression(parentSchema, name, getClass());
	}

	@Override
	public boolean isMutable() {
		return true;
	}

}
