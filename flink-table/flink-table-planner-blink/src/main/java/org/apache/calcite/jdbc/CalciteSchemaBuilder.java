package org.apache.calcite.jdbc;

import org.apache.calcite.schema.Schema;

/**
 * This class is used to create a {@link CalciteSchema} with a given {@link Schema} as the root.
 */
public class CalciteSchemaBuilder {

	/**
	 * Creates a {@link CalciteSchema} with a given {@link Schema} as the root.
	 *
	 * @param root schema to use as a root schema
	 * @return calcite schema with given schema as the root
	 */
	public static CalciteSchema asRootSchema(Schema root) {
		return new SimpleCalciteSchema(null, root, "");
	}

	private CalciteSchemaBuilder() {
	}
}
