

package org.apache.flink.table.api;

import org.apache.flink.annotation.PublicEvolving;

/**
 * Exception for an operation on a nonexistent catalog.
 */
@PublicEvolving
public class CatalogNotExistException extends RuntimeException {

	public CatalogNotExistException(String catalogName) {
		this(catalogName, null);
	}

	public CatalogNotExistException(String catalogName, Throwable cause) {
		super("Catalog " + catalogName + " does not exist.", cause);
	}
}
