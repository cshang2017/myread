package org.apache.flink.table.planner.catalog;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogManager;

import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * Bridge between the {@link CatalogManager} and the {@link Schema}. This way we can query Flink's specific catalogs
 * from Calcite.
 *
 * <p>The mapping for {@link Catalog}s is modeled as a strict two-level reference structure for Flink in Calcite,
 * the full path of objects is of format [catalog_name].[db_name].[meta-object_name].
 */
@Internal
public class CatalogManagerCalciteSchema extends FlinkSchema {

	private final CatalogManager catalogManager;
	// Flag that tells if the current planner should work in a batch or streaming mode.
	private final boolean isStreamingMode;

	public CatalogManagerCalciteSchema(
			CatalogManager catalogManager,
			boolean isStreamingMode) {
		this.catalogManager = catalogManager;
		this.isStreamingMode = isStreamingMode;
	}

	@Override
	public Table getTable(String name) {
		return null;
	}

	@Override
	public Set<String> getTableNames() {
		return Collections.emptySet();
	}

	@Override
	public Schema getSubSchema(String name) {
		if (catalogManager.schemaExists(name)) {
			return new CatalogCalciteSchema(name, catalogManager, isStreamingMode);
		} else {
			return null;
		}
	}

	@Override
	public Set<String> getSubSchemaNames() {
		return new HashSet<>(catalogManager.listCatalogs());
	}

	@Override
	public Expression getExpression(SchemaPlus parentSchema, String name) {
		return null;
	}

	@Override
	public boolean isMutable() {
		return false;
	}

}
