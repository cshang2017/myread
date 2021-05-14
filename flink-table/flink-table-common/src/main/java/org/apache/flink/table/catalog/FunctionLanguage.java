package org.apache.flink.table.catalog;

import org.apache.flink.annotation.PublicEvolving;

/**
 * Categorizes the language semantics of a {@link CatalogFunction}.
 */
@PublicEvolving
public enum FunctionLanguage {

	JAVA,

	SCALA,

	PYTHON
}
