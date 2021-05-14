package org.apache.flink.table.catalog;

import java.util.Optional;

/**
 * Interface for a function in a catalog.
 */
public interface CatalogFunction {

	/**
	 * Get the full name of the class backing the function.
	 *
	 * @return the full name of the class
	 */
	String getClassName();

	/**
	 * Create a deep copy of the function.
	 *
	 * @return a deep copy of "this" instance
	 */
	CatalogFunction copy();

	/**
	 * Get a brief description of the function.
	 *
	 * @return an optional short description of the function
	 */
	Optional<String> getDescription();

	/**
	 * Get a detailed description of the function.
	 *
	 * @return an optional long description of the function
	 */
	Optional<String> getDetailedDescription();

	/**
	 * Distinguish if the function is a generic function.
	 *
	 * @return whether the function is a generic function
	 */
	boolean isGeneric();

	/**
	 * Get the language used for the definition of function.
	 *
	 * @return  the language type of the function definition
	 */
	FunctionLanguage getFunctionLanguage();
}
