package org.apache.flink.table.factories;

import org.apache.flink.table.catalog.CatalogFunction;
import org.apache.flink.table.functions.FunctionDefinition;

/**
 * A factory to create {@link FunctionDefinition}.
 */
public interface FunctionDefinitionFactory {

	/**
	 * Creates a {@link FunctionDefinition} from given {@link CatalogFunction}.
	 *
	 * @param name name of the {@link CatalogFunction}
	 * @param catalogFunction the catalog function
	 * @return a {@link FunctionDefinition}
	 */
	FunctionDefinition createFunctionDefinition(String name, CatalogFunction catalogFunction);
}
