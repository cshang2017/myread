package org.apache.flink.table.descriptors;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.table.descriptors.GenericInMemoryCatalogValidator.CATALOG_TYPE_VALUE_GENERIC_IN_MEMORY;

/**
 * Catalog descriptor for the generic in memory catalog.
 */
public class GenericInMemoryCatalogDescriptor extends CatalogDescriptor {

	public GenericInMemoryCatalogDescriptor() {
		super(CATALOG_TYPE_VALUE_GENERIC_IN_MEMORY, 1);
	}

	public GenericInMemoryCatalogDescriptor(String defaultDatabase) {
		super(CATALOG_TYPE_VALUE_GENERIC_IN_MEMORY, 1, defaultDatabase);
	}

	@Override
	protected Map<String, String> toCatalogProperties() {
		return Collections.unmodifiableMap(new HashMap<>());
	}
}
